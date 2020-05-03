package org.stormexample.Events;

import java.time.LocalTime;

public class VoltageEvent {
    /** Voltage in Volt. */
    private double voltage;

    /** Time Voltage reading was taken. */
    private LocalTime timeOfReading;

    /**
     * Single value constructor.
     *
     * @param value Voltage in Volt.
     */
    /**
     * Pressure constructor.
     *
     * @param voltage Voltage in Volt
     * @param timeOfReading Time of Reading
     */
    public VoltageEvent(double voltage, LocalTime timeOfReading) {
        this.voltage = voltage;
        this.timeOfReading = timeOfReading;
    }

    public VoltageEvent() {
    }
    /**
     * Get the Voltage.
     *
     * @return Voltage in Volt
     */
    public double getVoltage() {
        return voltage;
    }

    /**
     * Get time Voltage reading was taken.
     *
     * @return Time of Reading
     */
    public LocalTime getTimeOfReading() {
        return timeOfReading;
    }

    @Override
    public String toString() {
        return "VoltageEvent [" + voltage + "V]";
    }
}
