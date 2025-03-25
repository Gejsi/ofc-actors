package com.ofc;

import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends AbstractActor {
  private final int id;
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private List<ActorRef> processes;

  private Process(int id) {
    this.id = id;
  }

  public static Props props(int id) {
    return Props.create(Process.class, () -> new Process(id));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(SetProcesses.class, this::handleSetProcesses)
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

  public record Crash() implements Msg {
  }
}
