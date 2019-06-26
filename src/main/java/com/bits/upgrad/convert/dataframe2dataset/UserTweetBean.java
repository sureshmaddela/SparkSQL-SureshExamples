package com.bits.upgrad.convert.dataframe2dataset;

import java.io.Serializable;

public class UserTweetBean implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String plant;
	private String species;
	public String getPlant() {
		return plant;
	}
	public void setPlant(String plant) {
		this.plant = plant;
	}
	public String getSpecies() {
		return species;
	}
	public void setSpecies(String species) {
		this.species = species;
	}
	
	

}
