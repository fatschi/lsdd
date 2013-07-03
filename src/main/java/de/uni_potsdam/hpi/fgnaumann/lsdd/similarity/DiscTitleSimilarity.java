package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class DiscTitleSimilarity implements PositiveRule {
	private static DiscTitleSimilarity instance = null;
	private static AbstractStringMetric dist = new Levenshtein();

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
		return dist.getSimilarity(
				record1.getField(MultiBlocking.DISC_TITLE_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.DISC_TITLE_FIELD,
						PactString.class).getValue());
	}

	@Override
	public int getWeight() {
		return 4;
	}

	@Override
	public boolean matched(PactRecord record1, PactRecord record2) {
		// TODO Auto-generated method stub
		return false;
	}
}
