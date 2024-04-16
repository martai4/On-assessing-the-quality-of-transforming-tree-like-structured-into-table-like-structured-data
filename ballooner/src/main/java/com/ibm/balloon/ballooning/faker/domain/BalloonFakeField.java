package com.ibm.balloon.ballooning.faker.domain;

import com.ibm.balloon.ballooning.faker.BalloonEntry;
import lombok.Getter;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@Getter
public class BalloonFakeField extends BalloonEntry {
    private final Set<Object> possibleValues = new HashSet<>();

    @Override
    public Object drawObject(int knowledgeBaseSize) {
        Random rand = new Random();
        Object[] elements = possibleValues.toArray();
        int randomIndex = rand.nextInt(elements.length);

        double probability = (double) getKnowledgeBaseObjectAppearances() / knowledgeBaseSize;

        return (rand.nextInt(10_000) / 10_000.0 < probability)
                ? elements[randomIndex]
                : null;
    }
}
