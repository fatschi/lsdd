package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.ChapmanOrderedNameCompoundSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.CosineSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.DiceSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.EuclideanDistance;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Soundex;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class Combined2Similarity implements PositiveRule {
	private static Combined2Similarity instance = null;

	private Combined2Similarity() {
	}

	public static Combined2Similarity getInstance() {
		if (instance == null) {
			instance = new Combined2Similarity();
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

//	@Override
//	public boolean matched(PactRecord record1, PactRecord record2) {
//		String emptyString = "";
//		return greaterThan(
//				levenshtein(record1.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue(), record2.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue()),
//				ifThenElse(
//						greaterThan(
//								levenshtein(record2.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue(), record1.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue()),
//								ifThenElse(
//										greaterThan(
//												soundex(record2.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class).getValue(), emptyString),
//												0.7505004426448455),
//										levenshtein(record1.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue(),
//												record2.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue()),
//										0.7339006368323561)),
//						ifThenElse(
//								greaterThan(
//										euclidean(emptyString, record2.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue()),
//										ifThenElse(
//												greaterThan(
//														levenshtein(
//																record2.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue(),
//																record1.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue()),
//														0.7505004426448455),
//												ifThenElse(
//														greaterThan(
//																euclidean(
//																		emptyString,
//																		record2.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class).getValue()),
//																dice(record1.getField(MultiBlocking.GENRE_TITLE_FIELD, PactString.class).getValue(),
//																		record1.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue())),
//														jaroWinkler(
//																substring(
//																		record1.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue(),
//																		686673648,
//																		lengthOf(emptyString)),
//																emptyString),
//														0.30465158192619746),
//												0.7339006368323561)),
//								jaroWinkler(record2.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue(), emptyString),
//								0.30465158192619746), 0.7339006368323561));
//	}

	private static boolean greaterThan(double d1, double d2) {
		return d1 > d2;
	}

	private static int lengthOf(String s) {
		return s.length();
	}

	private static String substring(String s, int pos1, int pos2) {
		int length = s.length();
		if (pos1 > length) {
			pos1 = length - 1;
		}
		if (pos1 < 0) {
			pos1 = 0;
		}
		if (pos2 > length) {
			pos2 = length - 1;
		}
		if (pos2 < 0) {
			pos2 = 0;
		}
		if (pos1 <= pos2) {
			return s.substring(pos1, pos2);
		} else {
			return s.substring(pos2, pos1);
		}
	}

	private static double levenshtein(String discTitle, String discTitle2) {
		Levenshtein leven = new Levenshtein();
		return leven.getSimilarity(discTitle, discTitle2);
	}

	private static double dice(String discTitle, String discTitle2) {
		DiceSimilarity dice = new DiceSimilarity();
		return dice.getSimilarity(discTitle, discTitle2);
	}

	private static double chapman(String discTitle, String discTitle2) {
		ChapmanOrderedNameCompoundSimilarity chapman = new ChapmanOrderedNameCompoundSimilarity();
		return chapman.getSimilarity(discTitle, discTitle2);
	}

	private static double cosine(String discTitle, String discTitle2) {
		CosineSimilarity cosine = new CosineSimilarity();
		return cosine.getSimilarity(discTitle, discTitle2);
	}

	private static double euclidean(String discTitle, String discTitle2) {
		EuclideanDistance eucl = new EuclideanDistance();
		return eucl.getSimilarity(discTitle, discTitle2);
	}

	private static double jaccard(String discTitle, String discTitle2) {
		JaccardSimilarity jac = new JaccardSimilarity();
		return jac.getSimilarity(discTitle, discTitle2);
	}

	private static double jaroWinkler(String discTitle, String discTitle2) {
		JaroWinkler jaro = new JaroWinkler();
		return jaro.getSimilarity(discTitle, discTitle2);
	}

	private static double soundex(String discTitle, String discTitle2) {
		Soundex soundex = new Soundex();
		return soundex.getSimilarity(discTitle, discTitle2);
	}

	private static double ifThenElse(boolean b, double d1, double d2) {
		if (b)
			return d1;
		return d2;
	}
}
