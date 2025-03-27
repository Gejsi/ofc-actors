package com.ofc;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends AbstractActor {
  private final int id;
  private final int n;
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private List<ActorRef> processes;
  private int ballot;
  private int imposeBallot;
  private int readBallot;
  private Integer estimate;
  private Integer proposal;
  private Map<ActorRef, Gather> gatheredResponses = new HashMap<>();
  private Map<ActorRef, Ack> ackResponses = new HashMap<>();
  // actors are persistent, so we need explicit state
  // to enforce the process halts after deciding
  private boolean decided = false;

  private Process(int id, int n) {
    this.id = id;
    this.n = n;
    this.ballot = id - n;
    this.imposeBallot = id - n;
    this.readBallot = 0;
  }

  public static Props props(int id, int n) {
    return Props.create(Process.class, () -> new Process(id, n));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(SetProcesses.class, this::handleSetProcesses)
        .match(Propose.class, this::handlePropose)
        .match(Read.class, this::handleRead)
        .match(Gather.class, this::handleGather)
        .match(Impose.class, this::handleImpose)
        .match(Ack.class, this::handleAck)
        .match(Decide.class, this::handleDecide)
        .match(Abort.class, this::handleAbort)
        .match(Crash.class, msg -> {
          log.info("Process {} crashed", id);
          getContext().become(crashed());
        })
        .build();
  }

  private void handleSetProcesses(SetProcesses msg) {
    this.processes = msg.processes;
    log.info("Process {} initialized with all {} processes", id, processes.size());
  }

  private void handlePropose(Propose msg) {
    if (decided) {
      log.debug("Process {} already decided: halted proposal of {}", id, msg.value());
      return;
    }

    proposal = msg.value();
    ballot += n;
    gatheredResponses.clear();

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
      log.debug("[P{}] FILTERING stale messages while gathering from {}: {}", id, sender().path().name(), msg);
      return;
    }

    gatheredResponses.put(sender(), msg);
    log.debug("[P{}] GATHER response from {}: {}", id, sender().path().name(), msg);

    if (gatheredResponses.size() > processes.size() / 2) {
      Optional<Gather> maxEntry = gatheredResponses.values().stream()
          .filter(res -> res.estBallot() > 0)
          .max(Comparator.comparingInt(Gather::estBallot));

      maxEntry.ifPresent((g) -> {
        proposal = g.estimate();
        log.info("Process {} adopts proposal={} from ballot={}", id, proposal, g.estBallot());
      });

      gatheredResponses.clear();
      log.info("Process {} imposes proposal={} with ballot={}", id, proposal, ballot);
      broadcast(new Impose(ballot, proposal));
    }
  }

  private void handleImpose(Impose msg) {
    if (msg.ballot() < readBallot || msg.ballot() < imposeBallot) {
      log.debug("[P{}] REJECTING impose ballot={}", id, msg.ballot());
      sender().tell(new Abort(msg.ballot()), self());
    } else {
      imposeBallot = msg.ballot();
      estimate = msg.value();
      log.debug("[P{}] ACCEPTING impose ballot={} value={}", id, msg.ballot(), msg.value());
      sender().tell(new Ack(msg.ballot()), self());
    }
  }

  private void handleAck(Ack msg) {
    if (msg.ballot() != ballot) {
      log.debug("[P{}] FILTERING stale messages while acknowledging from {}: {}", id, sender().path().name(), msg);
      return;
    }

    ackResponses.put(sender(), msg);
    if (ackResponses.size() > processes.size() / 2) {
      log.info("Process {} decided {}", id, proposal);
      broadcast(new Decide(proposal));
      decided = true;
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
      log.debug("[P{}] FILTERING stale messages while aborting from {}: {}", id, sender().path().name(), msg);
      return;
    }

    log.info("Process {} aborted proposal with ballot {}", id, msg.ballot());
    gatheredResponses.clear();
    ackResponses.clear();
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

  // TODO: add messages for leader handling (like Hold, ElectLeader)
}
