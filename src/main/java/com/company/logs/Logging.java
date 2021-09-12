package com.company.logs;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Logging {

    public void log(Level Loglevel, String message)
    {
        try{
        	FileHandler fh = new FileHandler("log.txt",1024*100,1,true);
            Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
            LOGGER.addHandler(fh);
            LOGGER.setUseParentHandlers(false);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            LOGGER.log(Loglevel,message);
            fh.flush();
            fh.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
