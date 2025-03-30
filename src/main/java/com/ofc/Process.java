package com.ofc;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.FI;

public class Process extends AbstractActor {
  private final int id;
  private final int n;
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  // Synod state variables
  private int ballot;
  private int imposeBallot;
  private int readBallot;
  private Integer estimate;
  private Integer proposal;
  private Map<ActorRef, Gather> gatheredResponses = new HashMap<>();
  private Map<ActorRef, Ack> ackResponses = new HashMap<>();

  // Actor state
  private List<ActorRef> processes;
  private boolean decided = false;
  private Integer decidedValue;
  private boolean proposing = false;
  private boolean holding = false;
  private final CrashProxy crashProxy;
  private boolean launched = false;
  private Integer valueToLaunch = null;

  private Process(int id, int n, double crashProbability) {
    this.id = id;
    this.n = n;
    this.ballot = id - n;
    this.imposeBallot = id - n;
    this.readBallot = 0;
    this.crashProxy = new CrashProxy(crashProbability);
  }

  public static Props props(int id, int n, double crashProbability) {
    return Props.create(Process.class, () -> new Process(id, n, crashProbability));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Launch.class, this::handleLaunch)
        .match(Hold.class, this::handleHold)
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
        .build();
  }

  private void handleSetProcesses(SetProcesses msg) {
    this.processes = msg.processes();
    log.info("Process {} initialized with all {} processes", id, n);
  }

  private void handleLaunch(Launch msg) {
    if (launched || decided || holding) {
      return;
    }
    launched = true;
    valueToLaunch = new Random().nextInt(2);

    log.info("Process {} launched! Will repeatedly propose value: {}", id, valueToLaunch);

    self().tell(new Propose(valueToLaunch), self());
  }

  private void handlePropose(Propose msg) {
    if (proposing) {
      log.info("Process {} has a unresolved proposal: ignoring proposal {}", id, msg.value());
      return;
    }

    if (holding) {
      log.info("Process {} is holding: ignoring proposal {}", id, msg.value());
      return;
    }

    if (decided) {
      log.info("Process {} already decided: ignoring proposal {}", id, msg.value());
      return;
    }

    proposing = true;
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
    if (!proposing) {
      return;
    }

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
    if (!proposing) {
      return;
    }

    if (msg.ballot() != ballot) {
      log.debug("Ignoring ACK for old/future ballot {} (current is {})", msg.ballot(), ballot);
      return;
    }

    ackResponses.put(sender(), msg);

    if (ackResponses.size() > n / 2) {
      log.info("Process {} decided {}", id, proposal);
      setDecision(proposal);
      broadcast(new Decide(proposal));
      resetProposalState();
    }
  }

  private void handleDecide(Decide msg) {
    if (decided) {
      return;
    }

    log.info("Process {} adopted decision {}", id, msg.value());
    setDecision(msg.value());
    estimate = msg.value();
    broadcast(msg);

    // if we were proposing, this decision overrides our proposal attempt
    if (proposing) {
      log.debug("Proposal ballot {} preempted by received decision for value {}", this.ballot, msg.value());
      resetProposalState();
    }
  }

  private void handleAbort(Abort msg) {
    if (!proposing) {
      return;
    }

    if (msg.ballot() != ballot) {
      log.debug("Ignoring ABORT for old/future ballot {} (current is {})", msg.ballot(), ballot);
      return;
    }

    log.info("Process {} aborted proposal with ballot {}", id, msg.ballot());
    resetProposalState();

    if (launched && !holding && !decided) {
      log.info("Process {} scheduled retry proposal for value {} after abort", id, valueToLaunch);

      // self().tell(new Propose(valueToLaunch), getSelf());
      // NOTE: instead of proposing immediately we can add
      // a fixed amount of backoff; this is useful
      // when the leader hasn't been elected yet,
      // and the amount of contention would be to high with just ofcons
      getContext().getSystem().scheduler().scheduleOnce(
          Duration.ofMillis(100),
          self(),
          new Propose(valueToLaunch),
          getContext().getSystem().dispatcher(),
          self());
    }
  }

  private void handleHold(Hold msg) {
    if (decided || holding) {
      return;
    }

    log.info("Process {} entered holding state.", id);
    holding = true;

    if (proposing) {
      log.info("Process {} aborting own proposal due to HOLD command.", id);
      resetProposalState();
    }
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

  private void setDecision(Integer value) {
    if (!decided) {
      decided = true;
      decidedValue = value;
      // TODO: maybe send back to Main the result as a Message
      // to calculate accurately the latency? Or even better than Main
      // another class just to handle this, so we can avoid making Main an actor
      // getContext().parent().tell(new DecisionResult(id, value), getSelf());
    }
  }

  private void resetProposalState() {
    proposing = false;
    proposal = null;
    gatheredResponses.clear();
    ackResponses.clear();
  }

  private interface Msg {
  }

  public record SetProcesses(List<ActorRef> processes) implements Msg {
  }

  public record Launch() implements Msg {
  }

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

  private class CrashProxy {
    private final double probability;
    private boolean faultProne = false;
    private boolean crashed = false;

    public CrashProxy(double probability) {
      this.probability = probability;
    }

    public <T> FI.UnitApply<T> getCrashableHandler(FI.UnitApply<T> msgHandler) {
      return (msg) -> {
        if (crashed) {
          log.debug("Process {} ignoring message because it has crashed", id);
          return;
        }

        // try to crash with given probability
        if (faultProne && Math.random() < probability) {
          crashed = true;
          log.info("Process {} crashed", id);
          getContext().become(crashed());
          return;
        }

        msgHandler.apply(msg);
      };
    }

    public void enableCrash() {
      log.info("Process {} may crash with a probability of {}%", id, probability * 100);
      faultProne = true;
    }
  }
}
