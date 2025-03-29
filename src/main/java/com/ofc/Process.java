package com.ofc;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.FI;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Process extends AbstractActor {
    public static AtomicLong firstDecided = new AtomicLong(0);

    private final int id;
    private final int n;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private List<ActorRef> processes;
    private int ballot;
    private int imposeBallot;
    private int readBallot;
    private Integer estimate;
    private Integer proposal;
    private final double crashProbability;
    private Map<ActorRef, Gather> gatheredResponses = new HashMap<>();
    private Map<ActorRef, Ack> ackResponses = new HashMap<>();
    // actors are persistent, so we need explicit state
    // to enforce the process halts after deciding
    private boolean decided = false;

    // technical stuff
    private final CrashProxy crashProxy;
    private AtomicBoolean holdEnabled = new AtomicBoolean(false);
    private Lock proposeLock = new ReentrantLock();
    private Condition proposeCondition = proposeLock.newCondition();
    private boolean proposing = false;


    private Process(int id, int n, double crashProbability) {
        this.id = id;
        this.n = n;
        this.ballot = id - n;
        this.imposeBallot = id - n;
        this.readBallot = 0;
        this.crashProbability = crashProbability;
        this.crashProxy = new CrashProxy(crashProbability);
    }

    public static Props props(int id, int n, double crashProbability) {
        return Props.create(Process.class, () -> new Process(id, n, crashProbability));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Launch.class, this::handleLaunch)
                .match(SetProcesses.class, crashProxy.getCrashableHandler(this::handleSetProcesses))
                .match(Propose.class, crashProxy.getCrashableHandler(this::handlePropose))
                .match(Read.class, crashProxy.getCrashableHandler(this::handleRead))
                .match(Gather.class, crashProxy.getCrashableHandler(this::handleGather))
                .match(Impose.class, crashProxy.getCrashableHandler(this::handleImpose))
                .match(Ack.class, crashProxy.getCrashableHandler(this::handleAck))
                .match(Decide.class, crashProxy.getCrashableHandler(this::handleDecide))
                .match(Abort.class, crashProxy.getCrashableHandler(this::handleAbort))
                .match(Crash.class, msg -> {
                    crashProxy.enableCrash();
                })
                .match(Hold.class, msg -> {
                    log.info("Process {} received HOLD message", id);
                    holdEnabled.set(true);
                })
                .build();
    }

    private void startProposingThread() {
        int value = new Random().nextInt(2);

        new Thread(() -> {
            while (!decided && !holdEnabled.get()) {
                try {
                    proposeLock.lock();
                    while (proposing) {
                        proposeCondition.await();
                    }
                    proposing = true;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    proposeLock.unlock();
                }

                log.info("Process {} proposing {}", id, value);
                self().tell(new Propose(value), self());
            }

            log.info("Process {} halted", id);
        }).start();

    }

    private void handleLaunch(Launch msg) {
        log.info("Process {} launched", id);

        this.startProposingThread();
    }

    private void handleSetProcesses(SetProcesses msg) {
        this.processes = msg.processes;
        log.info("Process {} initialized with all {} processes", id, n);
    }

    private void handlePropose(Propose msg) {
        if (decided) {
            log.debug("Process {} already decided: halted proposal of {}", id, msg.value());
            return;
        }

        proposal = msg.value();
        ballot += n;
        gatheredResponses.clear();
        ackResponses.clear();

        broadcast(new Read(ballot));
        log.info("Process {} started proposal {} with ballot {}", id, proposal, ballot);
    }

    private void handleRead(Read msg) {
        if (msg.ballot() < readBallot || msg.ballot() < imposeBallot) {
            log.debug("[P{}] REJECTING read ballot={} (my read={}, impose={})",
                    id, msg.ballot(), readBallot, imposeBallot);

            sender().tell(new Abort(msg.ballot()), self());
        } else {
            readBallot = msg.ballot();
            log.debug("[P{}] ACCEPTING read ballot={}", id, msg.ballot());
            sender().tell(new Gather(msg.ballot(), imposeBallot, estimate), self());
        }
    }

    private void handleGather(Gather msg) {
        // filter stale messages, only the current ballot is processed
        if (msg.ballot() != ballot) {
            log.debug("Ignoring GATHER for old/future ballot {} (current is {})", msg.ballot(), ballot);
            return;
        }

        gatheredResponses.put(sender(), msg);

        if (gatheredResponses.size() > n / 2) {
            Optional<Gather> maxEntry = gatheredResponses.values().stream()
                    .filter(res -> res.estBallot() > 0)
                    .max(Comparator.comparingInt(Gather::estBallot));

            maxEntry.ifPresent((g) -> {
                proposal = g.estimate();
                log.info("Process {} adopts proposal={} from ballot={}", id, proposal, g.estBallot());
            });

            gatheredResponses.clear();
            log.info("Process {} imposed proposal={} with ballot={}", id, proposal, ballot);
            broadcast(new Impose(ballot, proposal));

            // reset proposing state
            proposeLock.lock();
            proposing = false;
            proposeCondition.signal();
            proposeLock.unlock();
        }
    }

    private void handleImpose(Impose msg) {
        if (msg.ballot() < readBallot || msg.ballot() < imposeBallot) {
            log.debug("[P{}] REJECTING impose ballot={}", id, msg.ballot());
            sender().tell(new Abort(msg.ballot()), self());
        } else {
            estimate = msg.value();
            imposeBallot = msg.ballot();
            readBallot = Math.max(readBallot, msg.ballot()); // accepting impose implies accepting reads up to this ballot
            log.debug("[P{}] ACCEPTING impose ballot={} value={}", id, msg.ballot(), msg.value());
            sender().tell(new Ack(msg.ballot()), self());
        }
    }

    private void handleAck(Ack msg) {
        if (msg.ballot() != ballot) {
            log.debug("Ignoring ACK for old/future ballot {} (current is {})", msg.ballot(), ballot);
            return;
        }

        ackResponses.put(sender(), msg);

        if (ackResponses.size() > n / 2) {
            log.info("Process {} decided {}", id, proposal);
            broadcast(new Decide(proposal));
            decided = true;

            if (firstDecided.compareAndSet(0, System.nanoTime())) {
                log.info("First process decided at {}", firstDecided.get());
            }
        }
    }

    private void handleDecide(Decide msg) {
        if (decided) {
            return;
        }

        log.info("Process {} adopted decision {}", id, msg.value());
        decided = true;
        estimate = msg.value();
        broadcast(msg);
    }

    private void handleAbort(Abort msg) {
        if (msg.ballot() != ballot) {
            log.debug("Ignoring ABORT for old/future ballot {} (current is {})", msg.ballot(), ballot);
            return;
        }

        log.info("Process {} aborted proposal with ballot {}", id, msg.ballot());
        gatheredResponses.clear();
        ackResponses.clear();

        // restart proposing
        proposeLock.lock();
        proposing = false;
        proposeCondition.signal();
        proposeLock.unlock();
    }

    private Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {
                })
                .build();
    }

    private void broadcast(Msg msg) {
        for (ActorRef process : processes) {
            process.tell(msg, getSelf());
        }
    }

    private interface Msg {
    }

    public record Launch() implements Msg {
    }

    public record SetProcesses(List<ActorRef> processes) implements Msg {
    }

    // TODO: accept a probability parameter instead of crashing immediately
    public record Crash() implements Msg {
    }

    public record Propose(int value) implements Msg {
    }

    public record Read(int ballot) implements Msg {
    }

    public record Gather(int ballot, Integer estBallot, Integer estimate) implements Msg {
    }

    public record Impose(int ballot, Integer value) implements Msg {
    }

    public record Ack(int ballot) implements Msg {
    }

    public record Decide(Integer value) implements Msg {
    }

    public record Abort(int ballot) implements Msg {
    }

    public record Hold() implements Msg {
    }

    // TODO: add messages for leader handling (like Hold, ElectLeader)

    // stuff for crash

    private class CrashProxy {
        private AtomicBoolean crashEnabled = new AtomicBoolean(false);
        private AtomicBoolean crashed = new AtomicBoolean(false);

        private final double probability;

        public CrashProxy(double probability) {
            this.probability = probability;
        }

        public <T> FI.UnitApply<T> getCrashableHandler(FI.UnitApply<T> msgHandler) {
            return (msg) -> {
                if (crashed.get()) {
                    log.info("Process {} ignoring message because it has crashed", id);
                    return;
                }

                // try to crash with given probability
                if (crashEnabled.get() && Math.random() < probability) {
                    crashed.set(true);
                    log.info("Process {} crashed", id);
                    getContext().become(crashed());

                    log.info("Process {} ignoring message because it has crashed", id);
                    return;
                }

                msgHandler.apply(msg);
            };
        }

        public void enableCrash() {
            crashEnabled.set(true);
        }
    }
}
