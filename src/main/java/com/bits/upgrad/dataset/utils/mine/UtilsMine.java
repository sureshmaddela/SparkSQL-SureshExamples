package com.bits.upgrad.dataset.utils.mine;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class UtilsMine {
	
	public static void ignoreLogsFromPackagesExceptError() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		Logger.getRootLogger().setLevel(Level.ERROR);
	}

}
