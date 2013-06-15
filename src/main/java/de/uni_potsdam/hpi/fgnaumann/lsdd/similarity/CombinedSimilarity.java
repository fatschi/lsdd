package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.DiceSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.EuclideanDistance;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class CombinedSimilarity implements PositiveRule {
	private static CombinedSimilarity instance = null;

	private static AbstractStringMetric euclidean = new EuclideanDistance();
	private static AbstractStringMetric dice = new DiceSimilarity();
	private static AbstractStringMetric levenshtein = new Levenshtein();

	private CombinedSimilarity() {
	}

	public static CombinedSimilarity getInstance() {
		if (instance == null) {
			instance = new CombinedSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		return 0f;
	}

	@Override
	public int getWeight() {
		return 0;
	}

	@Override
	public boolean matched(PactRecord record1, PactRecord record2) {
		return levenshtein.getSimilarity(
				record1.getField(MultiBlocking.DISC_TITLE_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.DISC_TITLE_FIELD,
						PactString.class).getValue()) > subMeasure1(record1,
				record2);
	}

	private float subMeasure1(PactRecord record1, PactRecord record2) {
		if (levenshtein.getSimilarity(
				record1.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue()) > 0.7339006368323571f) {

			if (euclidean.getSimilarity(
					"",
					record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
							PactString.class).getValue()) > subMeasure2(
					record1, record2)) {
				return 0; // jaroWinklerOf(record2.discTitle, "")
			} else {
				return 0.30465158192619746f;
			}
		} else {
			return 0.7339006368323561f;
		}
	}

	private float subMeasure2(PactRecord record1, PactRecord record2) {
		if (levenshtein.getSimilarity(
				record1.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue()) > 0.7505004426448455)
			if (euclidean.getSimilarity(
					"",
					record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
							PactString.class).getValue()) > dice.getSimilarity(
					record1.getField(MultiBlocking.GENRE_TITLE_FIELD,
							PactString.class).getValue(),
					record1.getField(MultiBlocking.DISC_TITLE_FIELD,
							PactString.class).getValue()))
				return 0; // jaroWinklerOf(record1.discTitle.substring(686673648,
							// 0), "");
			else
				return 0.30465158192619743f;
		else
			return 0.7339006368323561f;
	}
}
