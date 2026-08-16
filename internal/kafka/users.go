package kafka

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// SCRAM mechanisms, in the spelling Kafka uses on the wire and kafman shows.
const (
	ScramSHA256 = "SCRAM-SHA-256"
	ScramSHA512 = "SCRAM-SHA-512"
)

// DefaultScramIterations is what Kafka itself defaults to. Fewer than 4096 is
// refused by the broker.
const DefaultScramIterations int32 = 4096

// saltLength is the salt size in bytes. RFC 5802 sets no length; 24 bytes is
// what the Kafka tooling generates.
const saltLength = 24

// The credential error codes Kafka answers with. sarama's KError list stops at
// 90, so these are named here — otherwise they surface as "Unknown error, how
// did this happen? Error code = 93", which is exactly the message a person
// cannot act on.
const (
	errResourceNotFound       = sarama.KError(91)
	errDuplicateResource      = sarama.KError(92)
	errUnacceptableCredential = sarama.KError(93)
)

// ScramUser is one SASL/SCRAM principal.
//
// Kafka has no user object beyond its credentials: a user exists exactly as
// long as it has at least one credential, which is why deleting one means
// deleting every mechanism it holds.
type ScramUser struct {
	Name        string
	Credentials []ScramCredential
}

// ScramCredential is one mechanism registered for a user.
type ScramCredential struct {
	Mechanism  string
	Iterations int32
}

// Mechanisms lists the user's mechanisms in a stable order.
func (u ScramUser) Mechanisms() []string {
	names := make([]string, 0, len(u.Credentials))
	for _, credential := range u.Credentials {
		names = append(names, credential.Mechanism)
	}
	sort.Strings(names)
	return names
}

// Has reports whether the user holds a credential for the mechanism.
func (u ScramUser) Has(mechanism string) bool {
	for _, credential := range u.Credentials {
		if credential.Mechanism == mechanism {
			return true
		}
	}
	return false
}

// ScramUserSpec creates a user or changes its password.
//
// The password is carried by value and never logged, never stored and never put
// in an error message: it leaves this struct only as the salted hash sarama
// computes while encoding the request.
type ScramUserSpec struct {
	Name      string
	Mechanism string
	Password  string

	// Iterations of 0 means DefaultScramIterations.
	Iterations int32
}

// ListScramUsers returns every SCRAM principal known to the cluster.
func (c *Client) ListScramUsers(ctx context.Context) ([]ScramUser, error) {
	return run(ctx, func() ([]ScramUser, error) {
		maybeDelay("list scram users")

		// A nil list asks for every user; naming users would only describe those.
		results, err := c.admin.DescribeUserScramCredentials(nil)
		if err != nil {
			return nil, fmt.Errorf("list SCRAM users: %w", scramSupportError(err))
		}

		users := make([]ScramUser, 0, len(results))
		for _, result := range results {
			if result == nil {
				continue
			}
			// A per-user error is reported inside the response. RESOURCE_NOT_FOUND
			// means the user has no credentials, which is not a failure to report.
			if !isNoError(result.ErrorCode) && result.ErrorCode != errResourceNotFound {
				return nil, fmt.Errorf("describe SCRAM user %s: %w", result.User, result.ErrorCode)
			}

			user := ScramUser{Name: result.User}
			for _, info := range result.CredentialInfos {
				if info == nil {
					continue
				}
				user.Credentials = append(user.Credentials, ScramCredential{
					Mechanism:  mechanismName(info.Mechanism),
					Iterations: info.Iterations,
				})
			}
			if len(user.Credentials) == 0 {
				continue
			}
			sort.Slice(user.Credentials, func(i, j int) bool {
				return user.Credentials[i].Mechanism < user.Credentials[j].Mechanism
			})
			users = append(users, user)
		}

		sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
		return users, nil
	})
}

// UpsertScramUser creates a user or replaces the password of an existing one.
// Kafka makes no distinction between the two, and neither does this.
//
// The call returns once the credential is visible in the cluster's own listing.
// That is not the same as "the new password works everywhere and the old one
// does not": each broker applies the change to its own SASL state a moment
// later, and no API reports when. The UI says as much rather than promising an
// instant cut-off.
func (c *Client) UpsertScramUser(ctx context.Context, spec ScramUserSpec) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("upsert scram user")

		if err := validateUserSpec(spec); err != nil {
			return struct{}{}, err
		}

		mechanism, err := mechanismType(spec.Mechanism)
		if err != nil {
			return struct{}{}, err
		}

		iterations := spec.Iterations
		if iterations <= 0 {
			iterations = DefaultScramIterations
		}

		salt := make([]byte, saltLength)
		if _, err := rand.Read(salt); err != nil {
			return struct{}{}, fmt.Errorf("generate a salt: %w", err)
		}

		// sarama salts and hashes the password while encoding the request: the
		// password itself never goes over the wire.
		results, err := c.admin.UpsertUserScramCredentials([]sarama.AlterUserScramCredentialsUpsert{{
			Name:       spec.Name,
			Mechanism:  mechanism,
			Iterations: iterations,
			Salt:       salt,
			Password:   []byte(spec.Password),
		}})
		if err != nil {
			return struct{}{}, fmt.Errorf("set the credentials of %s: %w", spec.Name, scramSupportError(err))
		}
		if err := alterResultError(results, "set the credentials of"); err != nil {
			return struct{}{}, err
		}

		// The credential is written to the metadata log and read back from it, so
		// it is not visible the instant the write is accepted. Waiting here is what
		// makes the list the UI reloads afterwards show what just happened.
		return struct{}{}, c.awaitCredential(spec.Name, spec.Mechanism, true)
	})
	return err
}

// DeleteScramUser removes the given mechanisms of a user, which is what removing
// the user amounts to. Passing none removes every mechanism it holds.
func (c *Client) DeleteScramUser(ctx context.Context, name string, mechanisms []string) error {
	_, err := run(ctx, func() (struct{}, error) {
		maybeDelay("delete scram user")

		if len(mechanisms) == 0 {
			var err error
			if mechanisms, err = c.userMechanisms(name); err != nil {
				return struct{}{}, err
			}
		}
		if len(mechanisms) == 0 {
			return struct{}{}, fmt.Errorf("SCRAM user %s does not exist", name)
		}

		deletions := make([]sarama.AlterUserScramCredentialsDelete, 0, len(mechanisms))
		for _, mechanism := range mechanisms {
			typed, err := mechanismType(mechanism)
			if err != nil {
				return struct{}{}, err
			}
			deletions = append(deletions, sarama.AlterUserScramCredentialsDelete{
				Name: name, Mechanism: typed,
			})
		}

		results, err := c.admin.DeleteUserScramCredentials(deletions)
		if err != nil {
			return struct{}{}, fmt.Errorf("delete SCRAM user %s: %w", name, scramSupportError(err))
		}
		if err := alterResultError(results, "delete SCRAM user"); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, c.awaitCredential(name, mechanisms[len(mechanisms)-1], false)
	})
	return err
}

// awaitCredential waits until a credential change is visible, or reports that
// it did not become so. Kafka accepts the write before the metadata log has
// caught up, and a listing taken in between shows the state from before.
func (c *Client) awaitCredential(name, mechanism string, present bool) error {
	for range awaitAttempts {
		mechanisms, err := c.userMechanisms(name)
		if err == nil {
			// The spelling may be whatever the caller passed, so both sides go
			// through the same normalisation the configuration uses.
			want := config.NormaliseMechanism(mechanism)
			found := false
			for _, held := range mechanisms {
				if config.NormaliseMechanism(held) == want {
					found = true
					break
				}
			}
			if found == present {
				return nil
			}
		}
		time.Sleep(awaitInterval)
	}

	action := "removed"
	if present {
		action = "written"
	}
	return fmt.Errorf("the credential of %s was accepted but has not been %s after %v; the controller may still be working",
		name, action, awaitAttempts*awaitInterval)
}

// userMechanisms reads back the mechanisms a user currently holds.
func (c *Client) userMechanisms(name string) ([]string, error) {
	results, err := c.admin.DescribeUserScramCredentials([]string{name})
	if err != nil {
		return nil, fmt.Errorf("describe SCRAM user %s: %w", name, scramSupportError(err))
	}

	var mechanisms []string
	for _, result := range results {
		if result == nil || result.User != name {
			continue
		}
		for _, info := range result.CredentialInfos {
			if info != nil {
				mechanisms = append(mechanisms, mechanismName(info.Mechanism))
			}
		}
	}
	return mechanisms, nil
}

// ------------------------------------------------------------------ helpers ---

// validateUserSpec refuses locally what the broker would refuse remotely, and
// says why in the same words the form uses.
func validateUserSpec(spec ScramUserSpec) error {
	switch {
	case strings.TrimSpace(spec.Name) == "":
		return errors.New("a user needs a name")
	case spec.Password == "":
		return errors.New("a user needs a password")
	case spec.Iterations != 0 && spec.Iterations < DefaultScramIterations:
		return fmt.Errorf("the broker refuses fewer than %d iterations", DefaultScramIterations)
	}
	return nil
}

// alterResultError turns the per-user part of an alter response into one error.
// The results carry the reason; the call itself returns nil even when nothing
// was changed.
func alterResultError(results []*sarama.AlterUserScramCredentialsResult, action string) error {
	for _, result := range results {
		if result == nil || isNoError(result.ErrorCode) {
			continue
		}
		if explained := credentialError(result.ErrorCode); explained != "" {
			return fmt.Errorf("%s %s: %s", action, result.User, explained)
		}
		if result.ErrorMessage != nil && *result.ErrorMessage != "" {
			return fmt.Errorf("%s %s: %s", action, result.User, *result.ErrorMessage)
		}
		return fmt.Errorf("%s %s: %w", action, result.User, result.ErrorCode)
	}
	return nil
}

// credentialError names the codes sarama does not know, in words that say what
// to do about them.
func credentialError(code sarama.KError) string {
	switch code {
	case errResourceNotFound:
		return "the user has no credential for that mechanism"
	case errDuplicateResource:
		return "the user already has a credential for that mechanism"
	case errUnacceptableCredential:
		return "the broker rejected the credential; check the password and the iteration count"
	default:
		return ""
	}
}

// scramSupportError explains the refusal of a cluster too old for the API,
// which otherwise arrives as a bare version error.
func scramSupportError(err error) error {
	if errors.Is(err, sarama.ErrUnsupportedVersion) {
		return fmt.Errorf("%w — SCRAM credentials need Kafka 2.7 or newer", err)
	}
	return err
}

func mechanismName(mechanism sarama.ScramMechanismType) string {
	switch mechanism {
	case sarama.SCRAM_MECHANISM_SHA_256:
		return ScramSHA256
	case sarama.SCRAM_MECHANISM_SHA_512:
		return ScramSHA512
	default:
		return "unknown"
	}
}

// mechanismType accepts the same spellings the configuration does, so
// "scram_sha_512" from a config file and "SCRAM-SHA-512" from a form mean the
// same thing here too.
func mechanismType(mechanism string) (sarama.ScramMechanismType, error) {
	switch config.NormaliseMechanism(mechanism) {
	case "scram-sha-256":
		return sarama.SCRAM_MECHANISM_SHA_256, nil
	case "scram-sha-512":
		return sarama.SCRAM_MECHANISM_SHA_512, nil
	default:
		return sarama.SCRAM_MECHANISM_UNKNOWN,
			fmt.Errorf("unknown SCRAM mechanism %q; use %s or %s", mechanism, ScramSHA256, ScramSHA512)
	}
}
