package org.stormexample.Events;

import java.time.LocalTime;

public class PressureEvent {
    /** Pressure in Pascal. */
    private double pressure;

    /** Time Pascal reading was taken. */
    private LocalTime timeOfReading;

    /**
     * Single value constructor.
     *
     * @param value Pressure in Pascal.
     */
    /**
     * Pressure constructor.
     *
     * @param pressure Pressure in Pascal
     * @param timeOfReading Time of Reading
     */
    public PressureEvent() {
    }

    public PressureEvent(double pressure, LocalTime timeOfReading) {
        this.pressure = pressure;
        this.timeOfReading = timeOfReading;
    }

    /**
     * Get the Pressure.
     *
     * @return Pressure in Pascal
     */
    public double getPressure() {
        return pressure;
    }

    /**
     * Get time Pressure reading was taken.
     *
     * @return Time of Reading
     */
    public LocalTime getTimeOfReading() {
        return timeOfReading;
    }

    @Override
    public String toString() {
        return "PressureEvent [" + pressure + "Pa]";
    }
}
