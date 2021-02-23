package com.xu.zeromq.core;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.UUID;

public class MessageIdGenerator {

    public static final int StrategyUUID = 0;
    public static final int StrategyRandomDigital = 1;

    private int strategy = StrategyRandomDigital;
    private final SecureRandom secureRandom = new SecureRandom();

    public MessageIdGenerator() {
    }

    public MessageIdGenerator(int strategy) {
        this.strategy = strategy;
    }

    public String generate() {
        String id = "";
        switch (strategy) {
            case StrategyUUID:
                id = UUID.randomUUID().toString().substring(0, 8);
                break;
            case StrategyRandomDigital:
                id = new BigInteger(130, secureRandom).toString(10).substring(0, 8);
                break;
        }
        return id;
    }
}
