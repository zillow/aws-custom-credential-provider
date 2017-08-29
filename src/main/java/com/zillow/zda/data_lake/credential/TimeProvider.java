package com.zillow.zda.data_lake.credential;

/**
 * Abstraction of system time function for testing purpose
 */
interface TimeProvider {

    /**
     * See {@link System#currentTimeMillis}
     */
    long currentTimeMillis();
}
