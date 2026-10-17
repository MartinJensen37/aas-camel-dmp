/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasRepositoryClient;

/** Resolves immutable repository inputs into an ordered, connector-neutral mapping plan. */
public final class MappingPlanCompiler {
	public sealed interface Target permits DirectTarget, AidTarget { }
	public record DirectTarget(AasReferenceResolver.ResolvedElement resolved) implements Target { }
	public record AidTarget(AasReferenceResolver.AidTarget resolved) implements Target { }
	public record EndpointPlan(String id, Reference reference, Target target,
			WireModel.Presence<String> pollingInterval) {
		public EndpointPlan(String id, Reference reference, Target target) {
			this(id, reference, target, WireModel.Presence.absent());
		}
		@Override public String toString() { return "EndpointPlan[id="+id+"]"; }
	}
	public record MappingPlan(String identity, int listIndex, List<EndpointPlan> sources,
			List<EndpointPlan> sinks, WireModel.Presence<String> defaultPollingInterval,
			WireModel.Presence<WireModel.Blob> transformation,
			WireModel.Presence<List<EndpointPlan>> responseSources,
			WireModel.Presence<List<EndpointPlan>> responseSinks,
			WireModel.Presence<WireModel.Blob> responseTransformation,
			WireModel.Presence<List<String>> triggerSourceIds, List<Qualifier> qualifiers) {
		public MappingPlan {
			sources=List.copyOf(sources); sinks=List.copyOf(sinks); qualifiers=List.copyOf(qualifiers);
			if(responseSources.present() && responseSources.value()!=null) responseSources=WireModel.Presence.of(List.copyOf(responseSources.value()));
			if(responseSinks.present() && responseSinks.value()!=null) responseSinks=WireModel.Presence.of(List.copyOf(responseSinks.value()));
			if(triggerSourceIds.present() && triggerSourceIds.value()!=null) triggerSourceIds=WireModel.Presence.of(List.copyOf(triggerSourceIds.value()));
		}
		@Override public String toString() { return "MappingPlan[identity="+identity+", listIndex="+listIndex+"]"; }
	}
	public record Plan(String aimcSubmodelId, List<MappingPlan> mappings) {
		public Plan(List<MappingPlan> mappings) { this(null, mappings); }
		public Plan { mappings=List.copyOf(mappings); }
	}
	/** A submodel whose change can change the mapping. */
	public record Dependency(String kind, String id) { }
	public record DesiredMapping(String identity, int listIndex, MappingPlan plan,
			List<AasRepositoryClient.Result<?>> failures, Set<Dependency> dependencies,
			WireModel.Mapping input, WireModel.MappingFailure projectionFailure) {
		public DesiredMapping(String identity, int listIndex, MappingPlan plan,
				List<AasRepositoryClient.Result<?>> failures, Set<Dependency> dependencies) {
			this(identity, listIndex, plan, failures, dependencies, null, null);
		}
		public DesiredMapping { failures=List.copyOf(failures); dependencies=Set.copyOf(dependencies); }
		public boolean valid() { return plan!=null && failures.isEmpty(); }
		@Override public String toString() { return "DesiredMapping[identity="+identity+", valid="+valid()+"]"; }
	}
	public record DesiredPlan(String aimcSubmodelId, List<DesiredMapping> mappings) {
		public DesiredPlan { mappings=List.copyOf(mappings); }
		public Plan validPlan() { return new Plan(aimcSubmodelId,mappings.stream()
				.filter(DesiredMapping::valid).map(DesiredMapping::plan).toList()); }
	}

	private final AasReferenceResolver references;

	public MappingPlanCompiler(AasReferenceResolver references) { this.references=references; }

	public AasRepositoryClient.Result<Plan> compile(WireModel.Aimc aimc) {
		DesiredPlan desired=compileDesired(aimc);
		for(DesiredMapping mapping:desired.mappings()) if(!mapping.valid())return propagate(mapping.failures().get(0));
		return found(desired.validPlan());
	}

	/** Every endpoint reference of an AIMC, for grouped prefetching before compilation. */
	public static List<Reference> endpointReferences(WireModel.Aimc aimc) {
		List<Reference> references=new ArrayList<>();
		for(WireModel.Mapping mapping:aimc.mappings())
			for(WireModel.Endpoint endpoint:endpoints(mapping))references.add(endpoint.reference());
		return List.copyOf(references);
	}

	/** Isolated desired outcomes retain every authored anchor even when an earlier fetch fails. */
	public DesiredPlan compileDesired(WireModel.Aimc aimc) {
		List<DesiredMapping> outcomes=new ArrayList<>();
		for(WireModel.Mapping mapping:aimc.mappings()) {
			String identity=mapping.authoredId().filter(id -> !id.isBlank()).orElse("mapping_"+mapping.listIndex());
			Set<Dependency> dependencies=new LinkedHashSet<>();
			dependencies.add(new Dependency("submodel",aimc.submodel().getId()));
			for(WireModel.Endpoint endpoint:endpoints(mapping)) anchorDependency(endpoint.reference(),dependencies);
			List<AasRepositoryClient.Result<?>> failures=new ArrayList<>();
			List<EndpointPlan> sources=new ArrayList<>(),sinks=new ArrayList<>();
			for(WireModel.Endpoint endpoint:mapping.sources()) {
				var compiled=compileEndpoint(endpoint);
				if(!compiled.found())failures.add(compiled);else sources.add(compiled.value());
			}
			for(WireModel.Endpoint endpoint:mapping.sinks()) {
				var compiled=compileEndpoint(endpoint);
				if(!compiled.found())failures.add(compiled);else sinks.add(compiled.value());
			}
			var responseSourcesResult=compileOptional(mapping.responseSources());
			if(!responseSourcesResult.found())failures.add(responseSourcesResult);
			var responseSinksResult=compileOptional(mapping.responseSinks());
			if(!responseSinksResult.found())failures.add(responseSinksResult);
			List<EndpointPlan> resolved=new ArrayList<>(sources);resolved.addAll(sinks);
			if(responseSourcesResult.found() && responseSourcesResult.value().value()!=null)resolved.addAll(responseSourcesResult.value().value());
			if(responseSinksResult.found() && responseSinksResult.value().value()!=null)resolved.addAll(responseSinksResult.value().value());
			for(EndpointPlan endpoint:resolved)if(endpoint.target() instanceof DirectTarget target)
				metadataDependencies(target.resolved().element(),dependencies);
			MappingPlan plan=failures.isEmpty()?new MappingPlan(identity,mapping.listIndex(),sources,sinks,mapping.defaultPollingInterval(),
					mapping.transformation(),responseSourcesResult.value(),responseSinksResult.value(),mapping.responseTransformation(),
					mapping.triggerSourceIds(),Aas.nonNull(mapping.element().getQualifiers())):null;
			outcomes.add(new DesiredMapping(identity,mapping.listIndex(),plan,failures,dependencies,mapping,null));
		}
		for (WireModel.MappingFailure failure : aimc.mappingFailures()) {
			Set<Dependency> dependencies = new LinkedHashSet<>();
			dependencies.add(new Dependency("submodel", aimc.submodel().getId()));
			for (WireModel.RejectedReference rejected : failure.references()) anchorDependency(rejected.reference(), dependencies);
			outcomes.add(new DesiredMapping(failure.identity(), failure.listIndex(), null,
					List.of(invalid(failure.pointer() + ": invalid AIMC mapping structure")), dependencies,null,failure));
		}
		if (!aimc.mappingFailures().isEmpty()) outcomes.sort(java.util.Comparator.comparingInt(DesiredMapping::listIndex));
		return new DesiredPlan(aimc.submodel().getId(),outcomes);
	}

	private static List<WireModel.Endpoint> endpoints(WireModel.Mapping mapping) {
		List<WireModel.Endpoint> all=new ArrayList<>(mapping.sources());all.addAll(mapping.sinks());
		if(mapping.responseSources().value()!=null)all.addAll(mapping.responseSources().value());
		if(mapping.responseSinks().value()!=null)all.addAll(mapping.responseSinks().value());
		return all;
	}

	private static void anchorDependency(Reference reference,Set<Dependency> dependencies) {
		List<Key> keys=Aas.keys(reference);
		if(!keys.isEmpty() && "Submodel".equals(Aas.keyType(keys.get(0))))dependencies.add(new Dependency("submodel",keys.get(0).getValue()));
	}

	private static void metadataDependencies(SubmodelElement element,Set<Dependency> dependencies) {
		if(element instanceof ReferenceElement reference)anchorDependency(reference.getValue(),dependencies);
		for(SubmodelElement child:Aas.children(element))metadataDependencies(child,dependencies);
	}

	private AasRepositoryClient.Result<WireModel.Presence<List<EndpointPlan>>> compileOptional(
			WireModel.Presence<List<WireModel.Endpoint>> endpoints) {
		if(!endpoints.present())return found(WireModel.Presence.absent());
		if(endpoints.value()==null)return found(WireModel.Presence.of(null));
		List<EndpointPlan> result=new ArrayList<>();
		for(WireModel.Endpoint endpoint:endpoints.value()) {
			var compiled=compileEndpoint(endpoint);
			if(!compiled.found())return propagate(compiled);
			result.add(compiled.value());
		}
		return found(WireModel.Presence.of(List.copyOf(result)));
	}

	private AasRepositoryClient.Result<EndpointPlan> compileEndpoint(WireModel.Endpoint endpoint) {
		if(Aas.nonNull(endpoint.element().getQualifiers()).stream().anyMatch(qualifier -> qualifier.getKind()!=QualifierKind.TEMPLATE_QUALIFIER))
			return invalid("endpoint qualifier semantics are unsupported; refusing to discard them");
		var resolved=references.resolve(endpoint.reference());
		if(!resolved.found())return propagate(resolved);
		Target target=switch(resolved.value()) {
			case AasReferenceResolver.ResolvedElement element -> new DirectTarget(element);
			case AasReferenceResolver.AidTarget aid -> new AidTarget(aid);
		};
		return found(new EndpointPlan(endpoint.id(),endpoint.reference(),target,endpoint.pollingInterval()));
	}

	private static <T> AasRepositoryClient.Result<T> found(T value) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.FOUND,value,200,"");
	}
	private static <T> AasRepositoryClient.Result<T> invalid(String detail) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.INVALID_RESPONSE,null,0,detail);
	}
	private static <T> AasRepositoryClient.Result<T> propagate(AasRepositoryClient.Result<?> result) {
		return new AasRepositoryClient.Result<>(result.status(),null,result.httpStatus(),result.detail());
	}
}
