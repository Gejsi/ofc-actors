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
//    private static final int NUM_PROCESSES = 100;
//    private static final int FAULTY_PROCESSES = (NUM_PROCESSES - 1) / 2;
//    private static final double CRASH_PROBABILITY = 1;
//    private static final Duration LEADER_ELECTION_DELAY = Duration.ofMillis(10);


    public static void main(String[] args) {
        final var process_nums = new int[]{3, 10, 100};
        final var faulty_nums = new int[]{1, 4, 49};
        final var leader_timeouts = new Duration[]{Duration.ofMillis(500), Duration.ofMillis(1000), Duration.ofMillis(1500), Duration.ofMillis(2000)};
        final var crash_probs = new double[]{0, 0.1, 1};
        final var repeat_simulation_times = 5;
        var results = new Double[crash_probs.length][faulty_nums.length][process_nums.length][leader_timeouts.length];

        for (int c = 0; c < crash_probs.length; c++) {
            for (int n = 0; n < process_nums.length; n++) {
                for (int l = 0; l < leader_timeouts.length; l++) {
                    double sum = 0;
                    for (int i = 0; i < repeat_simulation_times; i++) {
                        sum += simulation_loop(process_nums[n], faulty_nums[n], crash_probs[c], leader_timeouts[l]);
                    }
                    double avg = sum / repeat_simulation_times;
                    results[c][n][n][l] = avg;

                    System.out.println("Finished: " + (c * process_nums.length * leader_timeouts.length + n * leader_timeouts.length + l) / (double) (crash_probs.length * process_nums.length * leader_timeouts.length) * 100 + "%");
                }
            }

        }

        for (int c = 0; c < crash_probs.length; c++) {
            System.out.println("Crash probability: " + crash_probs[c]);
            for (int n = 0; n < process_nums.length; n++) {
                System.out.println("Process number: " + process_nums[n]);
                for (int l = 0; l < leader_timeouts.length; l++) {
                    System.out.println("Leader timeout: " + leader_timeouts[l] + ": " + results[c][n][n][l] + " ms");
                }
            }

        }

    }

    public static double simulation_loop(int num_processes, int faulty_processes, double crash_probability, Duration leader_election_delay) {
        ActorSystem system = ActorSystem.create("ofc");
        var log = Logging.getLogger(system, Main.class);
//        system.eventStream().setLogLevel(Logging.ErrorLevel());

        // simple class that helps during
        // the debugging of "dead letters" (=unhandled messages)
        ActorRef deadLetterMonitor = system.actorOf(Props.create(DeadLetterMonitor.class));
        system.eventStream().subscribe(deadLetterMonitor, DeadLetter.class);

        List<ActorRef> processes = IntStream.range(0, num_processes).mapToObj(i -> system.actorOf(Process.props(i, num_processes, crash_probability), "process-" + i)).collect(Collectors.toList());

        // send process list to all actors
        processes.forEach(process -> process.tell(new Process.SetProcesses(processes), ActorRef.noSender()));

        Random random = new Random(12345);
        List<Integer> crashIndices = random.ints(0, num_processes).distinct().limit(faulty_processes).boxed().collect(Collectors.toList());

        long startTime = System.nanoTime();

        system.scheduler().scheduleOnce(leader_election_delay, () -> {
            List<ActorRef> candidates = processes.stream().filter(p -> !crashIndices.contains(processes.indexOf(p))).toList();
            ActorRef leader = candidates.get(random.nextInt(candidates.size()));

            log.info("Process {} became leader", processes.indexOf(leader));

            processes.stream().filter(p -> !p.equals(leader)).forEach(p -> {
                p.tell(new Process.Hold(), ActorRef.noSender());
            });
        }, system.dispatcher());

        for (int i = 0; i < num_processes; i++) {
            ActorRef process = processes.get(i);
            if (crashIndices.contains(i)) {
                process.tell(new Process.Crash(), ActorRef.noSender());
            } else {
                // propose random value (0 or 1)
                process.tell(new Process.Launch(), ActorRef.noSender());
            }
        }

        long latency = System.nanoTime() - startTime;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.info("Latency for N={}, f={}: {} ns", num_processes, faulty_processes, latency);
        System.out.println("Latency for N=" + num_processes + ", f=" + faulty_processes + ": " + latency + " ns");

        system.terminate();

        return latency;
    }
}
