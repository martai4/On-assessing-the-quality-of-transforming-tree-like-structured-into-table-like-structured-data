package com.ibm.balloon.ballooning.faker.domain;

import com.ibm.balloon.ballooning.faker.BalloonEntry;
import com.ibm.balloon.ballooning.faker.containers.ContainerFakeList;
import lombok.Getter;

import java.util.*;

@Getter
public class BalloonFakeList<T> extends BalloonEntry {
    private final Set<T> possibleValues = new HashSet<>();

    @Override
    public Object drawObject(int knowledgeBaseSize) {
        Random rand = new Random();
        List<T> elements = new ArrayList<>(possibleValues.stream().toList());
        int randomLength = rand.nextInt(elements.size());

        Collections.shuffle(elements);
        double probability = (double) getKnowledgeBaseObjectAppearances() / knowledgeBaseSize;

        return (rand.nextInt(10_000) / 10_000.0 < probability)
                ? new ContainerFakeList<>(elements.subList(0, randomLength))
                : null;
    }
}
