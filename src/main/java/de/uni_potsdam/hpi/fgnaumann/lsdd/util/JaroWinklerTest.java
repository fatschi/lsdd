package de.uni_potsdam.hpi.fgnaumann.lsdd.util;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;

public class JaroWinklerTest {
	public static void main(String args[]){
		JaroWinkler jaroWinkler = new JaroWinkler();
		System.out.println(jaroWinkler.getSimilarity("Blackbird", "Ob-La-Di, Ob-La-Da"));
	}

}
