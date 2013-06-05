package de.uni_potsdam.hpi.fgnaumann.lsdd;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class SimilarityMeasure {
	public static boolean isDuplicate(PactRecord record1, PactRecord record2){
		AbstractStringMetric dist = new Levenshtein();
		if (dist.getSimilarity(record1.getField(3, PactString.class)
				.getValue(), record2.getField(3, PactString.class)
				.getValue()) > 0.9) {
			return true;
		}
		return false;
	}
}
