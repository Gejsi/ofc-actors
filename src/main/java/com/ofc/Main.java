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

    private static final int NUM_PROCESSES = 10;
    private static final int FAULTY_PROCESSES = (NUM_PROCESSES - 1) / 2;
    private static final double CRASH_PROBABILITY = 0.1;
    private static final Duration LEADER_ELECTION_DELAY = Duration.ofMillis(500);


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
                log.info("Crashing process {}", i);
                process.tell(new Process.Crash(), ActorRef.noSender());
            } else {
                // propose random value (0 or 1)
                process.tell(new Process.Launch(), ActorRef.noSender());
            }
        }

        system.scheduler().scheduleOnce(
                LEADER_ELECTION_DELAY,
                () -> {
                    List<ActorRef> candidates = processes.stream()
                            .filter(p -> !crashIndices.contains(processes.indexOf(p)))
                            .toList();
                    ActorRef leader = candidates.get(random.nextInt(candidates.size()));

                    log.info("Leader is process {}", processes.indexOf(leader));

                    processes.stream()
                            .filter(p -> !p.equals(leader))
                            .forEach(p -> {
                                log.info("Process {} is holding", processes.indexOf(p));
                                p.tell(new Process.Hold(), ActorRef.noSender());
                            });
                },
                system.dispatcher());

        // timeout added to put the latency log as last,
        // this value may need to be increased depending on the
        // number of processes/operations
        try {
            while (Process.firstDecided.get() == 0) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long latency = Process.firstDecided.get() - startTime;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.info("Latency for N={}, f={}: {} ns", NUM_PROCESSES, FAULTY_PROCESSES, latency);

        system.terminate();
    }
}
