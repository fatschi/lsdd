package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class DiscTitleSimilarity implements PositiveRule {
	private static DiscTitleSimilarity instance = null;
	private static AbstractStringMetric dist1 = new Levenshtein();
	private static AbstractStringMetric dist2 = new JaroWinkler();
	private static AbstractStringMetric dist3 = new JaccardSimilarity();

	private DiscTitleSimilarity() {
	}

	public static DiscTitleSimilarity getInstance() {
		if (instance == null) {
			instance = new DiscTitleSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		String discTitle1 = record1.getField(MultiBlocking.DISC_TITLE_FIELD,
				PactString.class).getValue();
		String discTitle2 = record2.getField(MultiBlocking.DISC_TITLE_FIELD,
				PactString.class).getValue();

		return (dist1.getSimilarity(discTitle1, discTitle2)
				+ dist2.getSimilarity(discTitle1, discTitle2) + dist3
					.getSimilarity(discTitle1, discTitle2)) / 3;
	}

	@Override
	public int getWeight() {
		return 7;
	}
}
