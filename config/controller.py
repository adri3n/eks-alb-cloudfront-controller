import kopf
import boto3
import kubernetes
import os
import yaml
import time

# Kubernetes clients
kubernetes.config.load_incluster_config()
api = kubernetes.client.CustomObjectsApi()
networking_api = kubernetes.client.NetworkingV1Api()

# CRD info
CLOUD_FRONT_CRD_GROUP = "cloudfront.services.k8s.aws"
CLOUD_FRONT_CRD_VERSION = "v1alpha1"

# Mapping kind -> plural
PLURAL_MAP = {
    "Distribution": "distributions",
    "Function": "functions",
    "CachePolicy": "cachepolicies",
    "OriginRequestPolicy": "originrequestpolicies",
    "KeyGroup": "keygroups"
}

TEMPLATE_PATH = "/app/cloudfront-template.yaml"

def load_and_patch_template(namespace, ingress_name, alb_dns):
    with open(TEMPLATE_PATH) as f:
        docs = list(yaml.safe_load_all(f))

    origin_id = f"{namespace}-{ingress_name}-origin"
    patched_docs = []

    for doc in docs:
        kind = doc.get("kind")
        meta = doc.get("metadata", {})
        if kind not in PLURAL_MAP:
            continue  # ignore unknown kinds

        meta['namespace'] = namespace
        base_name = meta.get("name", "cf-object")
        meta['name'] = f"{namespace}-{ingress_name}-{base_name}"
        doc['metadata'] = meta

        if kind == "Distribution":
            doc['spec']['distributionConfig']['comment'] = f"CloudFront for ALB {namespace}-{ingress_name}"
            doc['spec']['distributionConfig']['origins']['items'][0]['id'] = origin_id
            doc['spec']['distributionConfig']['origins']['items'][0]['domainName'] = alb_dns
            doc['spec']['distributionConfig']['defaultCacheBehavior']['targetOriginId'] = origin_id

        patched_docs.append(doc)
    return patched_docs

def create_or_update_crd(doc, logger):
    kind = doc.get("kind")
    plural = PLURAL_MAP.get(kind)
    name = doc['metadata']['name']
    namespace = doc['metadata']['namespace']
    if not plural:
        return
    try:
        api.get_namespaced_custom_object(
            group=CLOUD_FRONT_CRD_GROUP,
            version=CLOUD_FRONT_CRD_VERSION,
            namespace=namespace,
            plural=plural,
            name=name
        )
        logger.info(f"{kind} {name} already exists")
    except kubernetes.client.exceptions.ApiException as e:
        if e.status != 404:
            logger.error(f"Error checking {kind}: {e}")
            return
        try:
            api.create_namespaced_custom_object(
                group=CLOUD_FRONT_CRD_GROUP,
                version=CLOUD_FRONT_CRD_VERSION,
                namespace=namespace,
                plural=plural,
                body=doc
            )
            logger.info(f"Created {kind} {name}")
        except kubernetes.client.exceptions.ApiException as e2:
            logger.error(f"Failed to create {kind} {name}: {e2}")

def patch_ingress(namespace, ingress_name, cf_domain, logger):
    try:
        networking_api.patch_namespaced_ingress(
            ingress_name, namespace,
            {"metadata": {"annotations": {"external-dns.alpha.kubernetes.io/target": cf_domain}}}
        )
        logger.info(f"Patched Ingress {ingress_name} with CF domain {cf_domain}")
    except Exception as e:
        logger.error(f"Failed to patch ingress: {e}")

def remove_crds_and_patch(namespace, ingress_name, logger):
    for kind, plural in PLURAL_MAP.items():
        name_prefix = f"{namespace}-{ingress_name}-"
        try:
            objs = api.list_namespaced_custom_object(
                group=CLOUD_FRONT_CRD_GROUP,
                version=CLOUD_FRONT_CRD_VERSION,
                namespace=namespace,
                plural=plural
            )
            for item in objs.get('items', []):
                if item['metadata']['name'].startswith(name_prefix):
                    api.delete_namespaced_custom_object(
                        group=CLOUD_FRONT_CRD_GROUP,
                        version=CLOUD_FRONT_CRD_VERSION,
                        namespace=namespace,
                        plural=plural,
                        name=item['metadata']['name']
                    )
                    logger.info(f"Deleted {kind} {item['metadata']['name']}")
        except Exception as e:
            logger.error(f"Error deleting {kind} objects: {e}")

    # remove external-dns annotation
    try:
        networking_api.patch_namespaced_ingress(
            ingress_name, namespace,
            {"metadata": {"annotations": {"external-dns.alpha.kubernetes.io/target": None}}}
        )
        logger.info(f"Removed external-dns.target from Ingress {ingress_name}")
    except Exception as e:
        logger.error(f"Failed to remove external-dns annotation: {e}")

@kopf.timer('networking.k8s.io', 'v1', 'ingresses', interval=60)
def reconcile_ingress(spec, status, meta, namespace, name, logger, **kwargs):
    annotations = meta.get("annotations", {})
    
    # Log pour indiquer l'Ingress et ses annotations
    logger.info(f"Processing Ingress: {name} in namespace: {namespace}")
    logger.info(f"Annotations: cloudfront.aws.k8s.io/enabled={annotations.get('cloudfront.aws.k8s.io/enabled', 'not set')}, "
                f"external-dns.alpha.kubernetes.io/target={annotations.get('external-dns.alpha.kubernetes.io/target', 'not set')}")

    cf_enabled = annotations.get("cloudfront.aws.k8s.io/enabled", "false").lower() == "true"

    if cf_enabled:
        try:
            # Récupérer l'Ingress pour obtenir le LoadBalancer
            ingress = networking_api.read_namespaced_ingress(name, namespace)
            alb_dns = ingress.status.load_balancer.ingress[0].hostname  # Récupérer le DNS du LoadBalancer
            logger.info(f"Retrieved LoadBalancer hostname: {alb_dns}")

            # Charger et patcher le template CloudFront
            docs = load_and_patch_template(namespace, name, alb_dns)
            #for doc in docs:
            #    create_or_update_crd(doc, logger)
            
            # Patcher l'Ingress avec le domaine CloudFront
            #patch_ingress(namespace, name, alb_dns, logger)
        except Exception as e:
            logger.error(f"Failed to retrieve LoadBalancer hostname for Ingress {name}: {e}")
    else:
        remove_crds_and_patch(namespace, name, logger)
