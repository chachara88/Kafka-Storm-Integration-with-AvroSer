package org.stormexample.Events;

import java.time.LocalTime;
import java.util.Date;

public class TemperatureEvent {
    /** Temperature in Celcius. */
    private double temperature;

    /** Time temerature reading was taken. */
    private LocalTime timeOfReading;

    /**
     * Single value constructor.
     *
     * @param value Temperature in Celsius.
     */
    /**
     * Temerature constructor.
     *
     * @param temperature Temperature in Celsius
     * @param timeOfReading Time of Reading
     */
    public TemperatureEvent(){
    }

    public TemperatureEvent(double temperature, LocalTime timeOfReading) {
        this.temperature = temperature;
        this.timeOfReading = timeOfReading;
    }

    /**
     * Get the Temperature.
     *
     * @return Temperature in Celsius
     */
    public double getTemperature() {
        return temperature;
    }

    /**
     * Get time Temperature reading was taken.
     *
     * @return Time of Reading
     */
    public LocalTime getTimeOfReading() {
        return timeOfReading;
    }

    @Override
    public String toString() {
        return "TemperatureEvent [" + temperature + "C]";
    }

}
