package com.ofc;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.event.Logging;

public class Main {
  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("ofc");
    var log = Logging.getLogger(system, Main.class);

    // simple class that helps during
    // the debugging of "dead letters" (=unhandled messages)
    ActorRef deadLetterMonitor = system.actorOf(Props.create(DeadLetterMonitor.class));
    system.eventStream().subscribe(deadLetterMonitor, DeadLetter.class);

    final int NUM_PROCESSES = 3;
    final int FAULTY_PROCESSES = 1;

    List<ActorRef> processes = IntStream.range(0, NUM_PROCESSES)
        .mapToObj(i -> system.actorOf(Process.props(i), "process-" + i))
        .collect(Collectors.toList());

    // send process list to all actors
    processes.forEach(process -> process.tell(new Process.SetProcesses(processes), ActorRef.noSender()));

    long seed = 12345;
    List<Integer> crashIndices = new Random(seed).ints(0, NUM_PROCESSES)
        .distinct().limit(FAULTY_PROCESSES).boxed()
        .collect(Collectors.toList());

    long startTime = System.nanoTime();

    for (int i = 0; i < NUM_PROCESSES; i++) {
      ActorRef process = processes.get(i);
      if (crashIndices.contains(i)) {
        process.tell(new Process.Crash(), ActorRef.noSender());
      }
    }

    long endTime = System.nanoTime();
    long latency = endTime - startTime;

    // timeout added to put the latency log as last,
    // this value may need to be increased depending on the
    // number of processes/operations
    // try {
    // Thread.sleep(Duration.ofSeconds(1));
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }

    log.info("Latency for N={}, f={}: {} ns", NUM_PROCESSES, FAULTY_PROCESSES, latency);

    system.terminate();
  }
}
