package com.jrobertgardzinski.offboarding.control;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

/** Runs the offboarding specs through the real router with in-memory adapters. */
@Suite
@IncludeEngines("cucumber")
// one Gherkin file per use case in the top-level specs/ dir (build-helper puts them on the
// classpath root) — begin, record, sweep: the three doors into the process manager
@SelectClasspathResource("begin-offboarding.feature")
@SelectClasspathResource("record-confirmation.feature")
@SelectClasspathResource("sweep-overdue.feature")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.jrobertgardzinski.offboarding.control.appsteps")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME,
        value = "pretty, io.qameta.allure.cucumber7jvm.AllureCucumber7Jvm")
public class ApplicationBddTest {
}
