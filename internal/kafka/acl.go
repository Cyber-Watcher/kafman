package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// The vocabulary of an ACL, in Kafka's own spelling.
//
// kafman carries these as strings rather than sarama's enums: they are typed by
// people, shown in tables and compared, and sarama declares String() on a
// pointer receiver, so a value in a struct field does not stringify at all.
var (
	// ACLResourceTypes are the resources an ACL can be attached to.
	ACLResourceTypes = []string{"Topic", "Group", "Cluster", "TransactionalID", "DelegationToken"}

	// ACLPatternTypes decide whether ResourceName is a name or a prefix.
	ACLPatternTypes = []string{"Literal", "Prefixed"}

	// ACLOperations are what a principal may do.
	ACLOperations = []string{
		"All", "Read", "Write", "Create", "Delete", "Alter", "Describe",
		"ClusterAction", "DescribeConfigs", "AlterConfigs", "IdempotentWrite",
	}

	// ACLPermissions are the two answers an ACL gives.
	ACLPermissions = []string{"Allow", "Deny"}
)

// ACL is one access control entry: a principal may or may not perform an
// operation on a resource, from a host.
type ACL struct {
	Principal  string
	Host       string
	Operation  string
	Permission string

	ResourceType string
	ResourceName string
	PatternType  string
}

// Denies reports whether the entry is a denial, which is the row that has to
// stand out: a single Deny overrides every Allow.
func (a ACL) Denies() bool { return strings.EqualFold(a.Permission, "Deny") }

// Resource renders the resource the way Kafka names it, prefix pattern included.
func (a ACL) Resource() string {
	if strings.EqualFold(a.PatternType, "Prefixed") {
		return a.ResourceName + "*"
	}
	return a.ResourceName
}

// ACLFilter narrows a listing. Empty fields match anything.
type ACLFilter struct {
	Principal    string
	ResourceType string
	ResourceName string
}

// ListACLs returns the entries matching the filter.
func (c *Client) ListACLs(ctx context.Context, filter ACLFilter) ([]ACL, error) {
	return run(ctx, func() ([]ACL, error) {
		maybeDelay("list acls")

		saramaFilter := sarama.AclFilter{
			Version:                   1,
			ResourceType:              sarama.AclResourceAny,
			ResourcePatternTypeFilter: sarama.AclPatternAny,
			Operation:                 sarama.AclOperationAny,
			PermissionType:            sarama.AclPermissionAny,
		}
		if filter.Principal != "" {
			principal := filter.Principal
			saramaFilter.Principal = &principal
		}
		if filter.ResourceName != "" {
			name := filter.ResourceName
			saramaFilter.ResourceName = &name
		}
		if filter.ResourceType != "" {
			resourceType, err := aclResourceType(filter.ResourceType)
			if err != nil {
				return nil, err
			}
			saramaFilter.ResourceType = resourceType
		}

		resources, err := c.admin.ListAcls(saramaFilter)
		if err != nil {
			return nil, fmt.Errorf("list ACLs: %w", authorizerError(err))
		}

		var acls []ACL
		for _, resource := range resources {
			for _, acl := range resource.Acls {
				if acl == nil {
					continue
				}
				acls = append(acls, ACL{
					Principal:    acl.Principal,
					Host:         acl.Host,
					Operation:    aclOperationName(acl.Operation),
					Permission:   aclPermissionName(acl.PermissionType),
					ResourceType: aclResourceTypeName(resource.ResourceType),
					ResourceName: resource.ResourceName,
					PatternType:  aclPatternTypeName(resource.ResourcePatternType),
				})
			}
		}

		sortACLs(acls)
		return acls, nil
	})
}

// CreateACL adds one entry. Kafka accepts a duplicate silently, so creating an
// entry that already exists is not an error here either.
func (c *Client) CreateACL(ctx context.Context, acl ACL) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("create acl")

		resource, entry, err := acl.toSarama()
		if err != nil {
			return struct{}{}, err
		}

		if err := c.admin.CreateACLs([]*sarama.ResourceAcls{{
			Resource: resource,
			Acls:     []*sarama.Acl{&entry},
		}}); err != nil {
			return struct{}{}, fmt.Errorf("create the ACL: %w", authorizerError(err))
		}

		// The rule is written to the metadata log and read back from it, so a
		// listing taken right away can still show the state from before.
		return struct{}{}, c.awaitACL(resource, entry, true)
	})
	return err
}

// DeleteACL removes the entries matching exactly the one given, and reports how
// many were removed.
//
// The filter is built from every field of the entry, pattern type included:
// deleting "the Literal topic orders.events" must not also remove the Prefixed
// rule that happens to share its name.
func (c *Client) DeleteACL(ctx context.Context, acl ACL) (int, error) {
	return run(ctx, func() (int, error) {
		maybeDelay("delete acl")

		resource, entry, err := acl.toSarama()
		if err != nil {
			return 0, err
		}

		matched, err := c.admin.DeleteACL(exactFilter(resource, entry), false)
		if err != nil {
			return 0, fmt.Errorf("delete the ACL: %w", authorizerError(err))
		}

		for _, match := range matched {
			if !isNoError(match.Err) {
				return 0, fmt.Errorf("delete the ACL: %w", match.Err)
			}
		}
		if len(matched) == 0 {
			return 0, nil
		}
		return len(matched), c.awaitACL(resource, entry, false)
	})
}

// exactFilter matches one entry and nothing else. Every field is pinned,
// pattern type included: "the Literal topic orders.events" and "the Prefixed
// one" are different rules that happen to share a name.
func exactFilter(resource sarama.Resource, entry sarama.Acl) sarama.AclFilter {
	name, principal, host := resource.ResourceName, entry.Principal, entry.Host
	return sarama.AclFilter{
		Version:                   1,
		ResourceType:              resource.ResourceType,
		ResourceName:              &name,
		ResourcePatternTypeFilter: resource.ResourcePatternType,
		Principal:                 &principal,
		Host:                      &host,
		Operation:                 entry.Operation,
		PermissionType:            entry.PermissionType,
	}
}

// awaitACL waits until a rule change is visible to the authorizer, or reports
// that it did not become so.
func (c *Client) awaitACL(resource sarama.Resource, entry sarama.Acl, present bool) error {
	filter := exactFilter(resource, entry)

	for range awaitAttempts {
		resources, err := c.admin.ListAcls(filter)
		if err == nil {
			found := 0
			for _, r := range resources {
				found += len(r.Acls)
			}
			if (found > 0) == present {
				return nil
			}
		}
		time.Sleep(awaitInterval)
	}

	action := "removed"
	if present {
		action = "created"
	}
	return fmt.Errorf("the ACL for %s was accepted but has not been %s after %v; the controller may still be working",
		entry.Principal, action, awaitAttempts*awaitInterval)
}

// ------------------------------------------------------------------ helpers ---

// toSarama converts an entry, validating every field on the way: a typo in an
// operation would otherwise reach the broker as "Unknown" and be refused with a
// message that names no field.
func (a ACL) toSarama() (sarama.Resource, sarama.Acl, error) {
	resourceType, err := aclResourceType(a.ResourceType)
	if err != nil {
		return sarama.Resource{}, sarama.Acl{}, err
	}
	patternType, err := aclPatternType(a.PatternType)
	if err != nil {
		return sarama.Resource{}, sarama.Acl{}, err
	}
	operation, err := aclOperation(a.Operation)
	if err != nil {
		return sarama.Resource{}, sarama.Acl{}, err
	}
	permission, err := aclPermission(a.Permission)
	if err != nil {
		return sarama.Resource{}, sarama.Acl{}, err
	}

	principal := strings.TrimSpace(a.Principal)
	if principal == "" {
		return sarama.Resource{}, sarama.Acl{}, errors.New("an ACL needs a principal, for example User:billing-worker")
	}
	// Kafka principals are "Type:name"; a bare name is almost always a user, and
	// silently accepting it would create a rule that never matches.
	if !strings.Contains(principal, ":") {
		return sarama.Resource{}, sarama.Acl{}, fmt.Errorf(
			"a principal is written Type:name, for example User:%s", principal)
	}

	name := strings.TrimSpace(a.ResourceName)
	if name == "" {
		if resourceType == sarama.AclResourceCluster {
			// The cluster is a single resource and Kafka names it "kafka-cluster".
			name = "kafka-cluster"
		} else {
			return sarama.Resource{}, sarama.Acl{}, errors.New("an ACL needs a resource name, or * for every resource")
		}
	}

	host := strings.TrimSpace(a.Host)
	if host == "" {
		host = "*"
	}

	return sarama.Resource{
			ResourceType:        resourceType,
			ResourceName:        name,
			ResourcePatternType: patternType,
		}, sarama.Acl{
			Principal:      principal,
			Host:           host,
			Operation:      operation,
			PermissionType: permission,
		}, nil
}

// sortACLs orders entries so a refresh cannot shuffle them: denials first,
// because they are the ones that change what a principal can do.
func sortACLs(acls []ACL) {
	sort.SliceStable(acls, func(i, j int) bool {
		a, b := acls[i], acls[j]
		if a.Denies() != b.Denies() {
			return a.Denies()
		}
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		if a.ResourceName != b.ResourceName {
			return a.ResourceName < b.ResourceName
		}
		return a.Operation < b.Operation
	})
}

// authorizerError explains the refusal of a cluster with no authorizer, which
// otherwise arrives as a bare "security features are disabled".
func authorizerError(err error) error {
	if errors.Is(err, sarama.ErrSecurityDisabled) || mentions(err, "security features are disabled") {
		return fmt.Errorf("%w — the cluster has no authorizer configured", err)
	}
	return err
}

func aclResourceType(name string) (sarama.AclResourceType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "topic":
		return sarama.AclResourceTopic, nil
	case "group":
		return sarama.AclResourceGroup, nil
	case "cluster":
		return sarama.AclResourceCluster, nil
	case "transactionalid", "transactional-id":
		return sarama.AclResourceTransactionalID, nil
	case "delegationtoken", "delegation-token":
		return sarama.AclResourceDelegationToken, nil
	case "any", "":
		return sarama.AclResourceAny, nil
	default:
		return sarama.AclResourceUnknown,
			fmt.Errorf("unknown resource type %q; use one of %s", name, strings.Join(ACLResourceTypes, ", "))
	}
}

func aclResourceTypeName(resourceType sarama.AclResourceType) string {
	switch resourceType {
	case sarama.AclResourceTopic:
		return "Topic"
	case sarama.AclResourceGroup:
		return "Group"
	case sarama.AclResourceCluster:
		return "Cluster"
	case sarama.AclResourceTransactionalID:
		return "TransactionalID"
	case sarama.AclResourceDelegationToken:
		return "DelegationToken"
	default:
		return "Unknown"
	}
}

func aclPatternType(name string) (sarama.AclResourcePatternType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "literal", "":
		return sarama.AclPatternLiteral, nil
	case "prefixed":
		return sarama.AclPatternPrefixed, nil
	case "any":
		return sarama.AclPatternAny, nil
	case "match":
		return sarama.AclPatternMatch, nil
	default:
		return sarama.AclPatternUnknown,
			fmt.Errorf("unknown pattern type %q; use Literal or Prefixed", name)
	}
}

func aclPatternTypeName(patternType sarama.AclResourcePatternType) string {
	switch patternType {
	case sarama.AclPatternLiteral:
		return "Literal"
	case sarama.AclPatternPrefixed:
		return "Prefixed"
	case sarama.AclPatternMatch:
		return "Match"
	case sarama.AclPatternAny:
		return "Any"
	default:
		return "Unknown"
	}
}

func aclOperation(name string) (sarama.AclOperation, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "all":
		return sarama.AclOperationAll, nil
	case "read":
		return sarama.AclOperationRead, nil
	case "write":
		return sarama.AclOperationWrite, nil
	case "create":
		return sarama.AclOperationCreate, nil
	case "delete":
		return sarama.AclOperationDelete, nil
	case "alter":
		return sarama.AclOperationAlter, nil
	case "describe":
		return sarama.AclOperationDescribe, nil
	case "clusteraction", "cluster-action":
		return sarama.AclOperationClusterAction, nil
	case "describeconfigs", "describe-configs":
		return sarama.AclOperationDescribeConfigs, nil
	case "alterconfigs", "alter-configs":
		return sarama.AclOperationAlterConfigs, nil
	case "idempotentwrite", "idempotent-write":
		return sarama.AclOperationIdempotentWrite, nil
	case "any", "":
		return sarama.AclOperationAny, nil
	default:
		return sarama.AclOperationUnknown,
			fmt.Errorf("unknown operation %q; use one of %s", name, strings.Join(ACLOperations, ", "))
	}
}

func aclOperationName(operation sarama.AclOperation) string {
	switch operation {
	case sarama.AclOperationAll:
		return "All"
	case sarama.AclOperationRead:
		return "Read"
	case sarama.AclOperationWrite:
		return "Write"
	case sarama.AclOperationCreate:
		return "Create"
	case sarama.AclOperationDelete:
		return "Delete"
	case sarama.AclOperationAlter:
		return "Alter"
	case sarama.AclOperationDescribe:
		return "Describe"
	case sarama.AclOperationClusterAction:
		return "ClusterAction"
	case sarama.AclOperationDescribeConfigs:
		return "DescribeConfigs"
	case sarama.AclOperationAlterConfigs:
		return "AlterConfigs"
	case sarama.AclOperationIdempotentWrite:
		return "IdempotentWrite"
	case sarama.AclOperationAny:
		return "Any"
	default:
		return "Unknown"
	}
}

func aclPermission(name string) (sarama.AclPermissionType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "allow", "":
		return sarama.AclPermissionAllow, nil
	case "deny":
		return sarama.AclPermissionDeny, nil
	case "any":
		return sarama.AclPermissionAny, nil
	default:
		return sarama.AclPermissionUnknown,
			fmt.Errorf("unknown permission %q; use Allow or Deny", name)
	}
}

func aclPermissionName(permission sarama.AclPermissionType) string {
	switch permission {
	case sarama.AclPermissionAllow:
		return "Allow"
	case sarama.AclPermissionDeny:
		return "Deny"
	case sarama.AclPermissionAny:
		return "Any"
	default:
		return "Unknown"
	}
}
