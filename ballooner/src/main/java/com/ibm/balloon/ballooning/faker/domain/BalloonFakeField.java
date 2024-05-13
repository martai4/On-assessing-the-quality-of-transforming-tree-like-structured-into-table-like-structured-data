package com.ibm.balloon.ballooning.faker.domain;

import com.ibm.balloon.ballooning.faker.BalloonEntry;
import lombok.Getter;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@Getter
public class BalloonFakeField extends BalloonEntry {
    private final Random rand = new Random();
    private final Set<Object> possibleValues = new HashSet<>();
    private Object[] elements;

    @Override
    public Object drawObject(int knowledgeBaseSize) {
        int randomIndex = rand.nextInt(elements.length);
        double probability = (double) getKnowledgeBaseObjectAppearances() / knowledgeBaseSize;

        return (rand.nextInt(10_000) / 10_000.0 < probability)
                ? elements[randomIndex]
                : null;
    }

    public void setElements() {
        this.elements = possibleValues.toArray();
    }
}
