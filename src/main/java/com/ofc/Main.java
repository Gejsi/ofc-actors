package com.ofc;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.event.Logging;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

  private static final int NUM_PROCESSES = 20;
  private static final int FAULTY_PROCESSES = (NUM_PROCESSES - 1) / 2;
  private static final double CRASH_PROBABILITY = 1;
  private static final Duration LEADER_ELECTION_DELAY = Duration.ofMillis(0);

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("ofc");
    var log = Logging.getLogger(system, Main.class);

    // simple class that helps during
    // the debugging of "dead letters" (=unhandled messages)
    ActorRef deadLetterMonitor = system.actorOf(Props.create(DeadLetterMonitor.class));
    system.eventStream().subscribe(deadLetterMonitor, DeadLetter.class);

    List<ActorRef> processes = IntStream.range(0, NUM_PROCESSES)
        .mapToObj(i -> system.actorOf(Process.props(i, NUM_PROCESSES, CRASH_PROBABILITY), "process-" + i))
        .collect(Collectors.toList());

    // send process list to all actors
    processes.forEach(process -> process.tell(new Process.SetProcesses(processes), ActorRef.noSender()));

    Random random = new Random(12345);
    List<Integer> crashIndices = random.ints(0, NUM_PROCESSES)
        .distinct().limit(FAULTY_PROCESSES).boxed()
        .collect(Collectors.toList());

    long startTime = System.nanoTime();

    for (int i = 0; i < NUM_PROCESSES; i++) {
      ActorRef process = processes.get(i);
      if (crashIndices.contains(i)) {
        process.tell(new Process.Crash(), ActorRef.noSender());
      } else {
        // propose random value (0 or 1)
        process.tell(new Process.Propose(new Random().nextInt(2)), ActorRef.noSender());
      }
    }

    system.scheduler().scheduleOnce(
        LEADER_ELECTION_DELAY,
        () -> {
          List<ActorRef> candidates = processes.stream()
              .filter(p -> !crashIndices.contains(processes.indexOf(p)))
              .toList();
          ActorRef leader = candidates.get(random.nextInt(candidates.size()));

          log.info("Process {} became leader", processes.indexOf(leader));

          processes.stream()
              .filter(p -> !p.equals(leader))
              .forEach(p -> {
                p.tell(new Process.Hold(), ActorRef.noSender());
              });

          leader.tell(new Process.Propose(69), ActorRef.noSender());
        },
        system.dispatcher());

    long latency = System.nanoTime() - startTime;

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    log.info("Latency for N={}, f={}: {} ns", NUM_PROCESSES, FAULTY_PROCESSES, latency);

    system.terminate();
  }
}
