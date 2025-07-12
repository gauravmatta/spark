package com.daimplant.first_spark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for checking if a word is boring or not.
 * Boring words are loaded from a resource file.
 */
public class Util 
{
	/**
	 * A set containing boring words.
	 * Boring words are loaded from a resource file.
	 */
	public static Set<String> borings = new HashSet<>();

	/**
	 * Static block to load boring words from a resource file.
	 * The file should be located in the classpath under /filters/boringwords.txt.
	 */
	static {

		InputStream is = Main.class.getResourceAsStream("/filters/boringwords.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		br.lines().forEach(borings::add);
	}

	/**
	 * Checks if a word is boring.
	 *
	 * @param word the word to check
	 * @return true if the word is boring, false otherwise
	 */
	public static boolean isBoring(String word)
	{
		return borings.contains(word);
	}

	/**
	 * Checks if a word is not boring.
	 *
	 * @param word the word to check
	 * @return true if the word is not boring, false otherwise
	 */
	public static boolean isNotBoring(String word)
	{
		return !isBoring(word);
	}
	
}
