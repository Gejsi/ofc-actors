package com.ofc;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.event.Logging;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("ofc");
        var log = Logging.getLogger(system, Main.class);

        // simple class that helps during
        // the debugging of "dead letters" (=unhandled messages)
        ActorRef deadLetterMonitor = system.actorOf(Props.create(DeadLetterMonitor.class));
        system.eventStream().subscribe(deadLetterMonitor, DeadLetter.class);

        final int NUM_PROCESSES = 10;
        final int FAULTY_PROCESSES = (NUM_PROCESSES - 1) / 2;

        List<ActorRef> processes = IntStream.range(0, NUM_PROCESSES)
                .mapToObj(i -> system.actorOf(Process.props(i, NUM_PROCESSES), "process-" + i))
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

        // TODO: add leader election
        // system.scheduler().scheduleOnce(
        // Duration.ofSeconds(2),
        // () -> {
        // List<ActorRef> candidates = processes.stream()
        // .filter(p -> !crashIndices.contains(processes.indexOf(p)))
        // .collect(Collectors.toList());
        // ActorRef leader = candidates.get(random.nextInt(candidates.size()));

        // processes.forEach(p -> p.tell(new Process.ElectLeader(leader),
        // ActorRef.noSender()));

        // processes.stream()
        // .filter(p -> !p.equals(leader))
        // .forEach(p -> p.tell(new Process.Hold(), ActorRef.noSender()));
        // },
        // system.dispatcher());

        long endTime = System.nanoTime();
        long latency = endTime - startTime;

        // timeout added to put the latency log as last,
        // this value may need to be increased depending on the
        // number of processes/operations
        // try {
        // Thread.sleep(Duration.ofSeconds(10));
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }

        log.info("Latency for N={}, f={}: {} ns", NUM_PROCESSES, FAULTY_PROCESSES, latency);

        system.terminate();
    }
}
