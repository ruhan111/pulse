package com.example.pulse;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * The package rules from the README, enforced. A dependency that breaks the architecture fails the
 * build instead of slipping in unnoticed.
 */
@AnalyzeClasses(packages = "com.example.pulse", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

	@ArchTest
	static final ArchRule eventDependsOnlyOnTheJdk = classes()
		.that().resideInAPackage("..pulse.event..")
		.should().onlyDependOnClassesThat().resideInAnyPackage("..pulse.event..", "java..")
		.because("the domain must stay independent of frameworks and infrastructure");

	@ArchTest
	static final ArchRule ingestionDoesNotKnowWhatHappensDownstream = noClasses()
		.that().resideInAPackage("..pulse.ingestion..")
		.should().dependOnClassesThat().resideInAnyPackage("..pulse.trend..", "..pulse.api..")
		.allowEmptyShould(true)
		.because("adapters only publish events into an EventSink");

	@ArchTest
	static final ArchRule sourceAdaptersAreSelfContained = noClasses()
		.that().resideOutsideOfPackage("..pulse.ingestion..")
		.should().dependOnClassesThat().resideInAPackage("..pulse.ingestion..")
		.because("sources are wired in by Spring and publish into EventSink; nothing calls them directly");

	@ArchTest
	static final ArchRule trendDoesNotKnowWhereEventsComeFrom = noClasses()
		.that().resideInAPackage("..pulse.trend..")
		.should().dependOnClassesThat().resideInAnyPackage("..pulse.ingestion..", "..pulse.api..");

	@ArchTest
	static final ArchRule onlyInfrastructureKnowsInfrastructure = noClasses()
		.that().resideOutsideOfPackage("..pulse.infrastructure..")
		.should().dependOnClassesThat().resideInAPackage("..pulse.infrastructure..")
		.because("dependencies point inward, so technology can be swapped");

	@ArchTest
	static final ArchRule packagesAreFreeOfCycles = slices()
		.matching("com.example.pulse.(*)..")
		.should().beFreeOfCycles();

}
