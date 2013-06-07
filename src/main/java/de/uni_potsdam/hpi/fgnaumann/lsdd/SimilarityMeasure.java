package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.HashSet;
import java.util.Set;

import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.ArtistNameSimilarity;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.DiscTitleSimilarity;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.NegativeRule;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.PositiveRule;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.ReleaseYearDifference;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.TrackNumberDifference;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.XOrKeywords;
import eu.stratosphere.pact.common.type.PactRecord;

public class SimilarityMeasure {;
	
	static Set<NegativeRule> negativeRules = new HashSet<NegativeRule>();
	static Set<PositiveRule> positiveRules = new HashSet<PositiveRule>();

	static {
		negativeRules.add(TrackNumberDifference.getInstance());
		negativeRules.add(XOrKeywords.getInstance());
		negativeRules.add(ReleaseYearDifference.getInstance());
		positiveRules.add(ArtistNameSimilarity.getInstance());
		positiveRules.add(DiscTitleSimilarity.getInstance());
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
		return (similarity/multiplierSum > MultiBlocking.SIMILARITY_THRESHOLD);
	}
}
