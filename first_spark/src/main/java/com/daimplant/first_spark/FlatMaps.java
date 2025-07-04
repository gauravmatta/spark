package com.daimplant.first_spark;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class FlatMaps {

    public static void main(String[] args) {

    }

    private static @NotNull List<String> getStrings() {
        List<String> inputData = new ArrayList<>();
        inputData.add("INFO: Running Spark version 4.0.0");
        inputData.add("INFO: OS info Linux, 6.14.9-300.fc42.x86_64, amd64");
        inputData.add("INFO: Java version 21.0.7");
        inputData.add("WARN: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable");
        inputData.add("INFO: ==============================================================");
        inputData.add("INFO: No custom resources configured for spark.driver.");
        inputData.add("INFO: Submitted application: First Spark Application");
        inputData.add("WARN: OutputCommitCoordinator stopped!");
        inputData.add("Error: Successfully stopped SparkContext");
        inputData.add("FATAL: Shutdown hook called");
        inputData.add("Error: Deleting directory /tmp/spark-3582b043-23bb-4775-b057-a8261a2cbbfe");
        return inputData;
    }
}
