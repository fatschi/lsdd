package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import java.util.HashSet;
import java.util.Set;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;

public class SimilarityMeasure {

	private static final double CONFIDENCE_WINDOW = 0.1;
	
	static Set<NegativeRule> negativeRules = new HashSet<NegativeRule>();
	static Set<PositiveRule> positiveRules = new HashSet<PositiveRule>();

	static {
		// negative rules
		negativeRules.add(TrackNumberDifference.getInstance());
		negativeRules.add(ReleaseYearDifference.getInstance());
		negativeRules.add(XORKeywords.getInstance());

		// positive rules
		positiveRules.add(ArtistNameSimilarity.getInstance());
		positiveRules.add(DiscTitleSimilarity.getInstance());
		positiveRules.add(TrackNumberSimilarity.getInstance());
		positiveRules.add(GenreSimilarity.getInstance());
		positiveRules.add(ReleaseYearSimilarity.getInstance());

//		positiveRules.add(AlwaysTrueSimilarity.getInstance());
	}

	public static boolean isDuplicate(PactRecord record1, PactRecord record2) {
		for (NegativeRule nr : negativeRules) {
			if (nr.duplicateRuledOut(record1, record2)) {
				return false;
			}
		}
		int multiplierSum = 0;
		float similarity = 0.0f;
		for (PositiveRule pr : positiveRules) {
			similarity += pr.similarity(record1, record2) * pr.getWeight();
			multiplierSum += pr.getWeight();
		}
		if (MultiBlocking.takeTracksIntoAccount && similarity / multiplierSum < MultiBlocking.SIMILARITY_THRESHOLD + CONFIDENCE_WINDOW
				&& similarity / multiplierSum > MultiBlocking.SIMILARITY_THRESHOLD - CONFIDENCE_WINDOW) {
			similarity += TracksSimilarity.getInstance().similarity(record1, record2) * TracksSimilarity.getInstance().getWeight();
			multiplierSum += TracksSimilarity.getInstance().getWeight();
		}

		if (similarity / multiplierSum > MultiBlocking.SIMILARITY_THRESHOLD) {
			return true;
		} else {
			return false;
		}

	}
}
