package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class ReleaseYearSimilarity implements PositiveRule {
	private static final int MAX_RELEASE_DIFFERENCE = 10;
	private static ReleaseYearSimilarity instance = null;

	private ReleaseYearSimilarity() {
	}

	public static ReleaseYearSimilarity getInstance() {
		if (instance == null) {
			instance = new ReleaseYearSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		String record1DiscRelease = record1
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		String record2DiscRelease = record2
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		if (!record1DiscRelease.isEmpty() && !record2DiscRelease.isEmpty()) {
			try {
				int releaseDifference = (Math.abs(Integer
						.valueOf(record1DiscRelease)
						- Integer.valueOf(record2DiscRelease)));
				final int maxDifference = MAX_RELEASE_DIFFERENCE;
				if (releaseDifference <= maxDifference) {
					return 1 - releaseDifference / maxDifference;
				} else {
					return 0;
				}
			} catch (NumberFormatException nfe) {
			}
		}
		return MultiBlocking.SIMILARITY_THRESHOLD;
	}

	@Override
	public int getWeight() {
		return 3;
	}
}
