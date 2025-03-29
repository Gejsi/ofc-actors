package com.ofc;

import akka.actor.AbstractActor;
import akka.actor.DeadLetter;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class DeadLetterMonitor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DeadLetter.class, deadLetter -> {
                    log.warning("Dead letter encountered => Message: {}, Sender: {}", deadLetter.message(), deadLetter.sender());
                })
                .build();
    }
}
