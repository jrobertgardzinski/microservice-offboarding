Feature: Offboarding — the portal cleans up after a leaving account
  The portal's process manager for account deletion, extracted from the identity service so
  identity stays reusable. Security announces the FACT that an account requested deletion;
  this service commands every configured content participant to purge, collects their
  confirmations, and announces the single outcome security waits for: the portal's content is
  purged, or the purge failed because someone never answered. Commands are idempotent BY
  DEFAULT (workspace ADR 0006 — enforced by the generic IdempotentCommandsTest, not restated
  per scenario); the scenarios below pin the message choreography.

  Scenario: A deletion request commands the content purge
    When security announces that alice@example.com requested deletion
    Then a purge command for alice@example.com goes out to the content services

  Scenario: The leaver's policy choices ride the command untouched
    When security announces that alice@example.com requested deletion choosing memes=DELETE and comments=ANONYMIZE_AUTHOR
    Then the purge command carries the choices memes=DELETE and comments=ANONYMIZE_AUTHOR

  Scenario: A second deletion request joins the case already underway
    Given security announced that alice@example.com requested deletion
    When security announces another deletion request for alice@example.com
    Then the purge command for alice@example.com is sent again
    And no outcome is announced yet

  Scenario: An early confirmation announces nothing yet
    Given security announced that alice@example.com requested deletion
    When memes confirms its purge for alice@example.com
    Then no outcome is announced yet

  Scenario: The last confirmation announces the portal purged
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    And comments confirmed its purge for alice@example.com
    When collections confirms its purge for alice@example.com
    Then the portal announces the content of alice@example.com purged

  Scenario: A confirmation may echo the purge command it answers
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    And comments confirmed its purge for alice@example.com
    When collections confirms its purge for alice@example.com echoing the purge command
    Then the portal announces the content of alice@example.com purged

  Scenario: A confirmation for nobody's saga is a stray, not an error
    When memes confirms its purge for nobody@example.com
    Then no outcome is announced yet

  Scenario: A deletion fact the portal cannot place is dropped, not the requests behind it
    When security announces a deletion request that names no account
    And security announces a deletion request with a garbled identity
    And security announces that alice@example.com requested deletion
    Then a purge command for alice@example.com goes out to the content services
    And no outcome is announced yet

  Scenario: Silence past the deadline retries the purge command before giving up
    Given security announced that alice@example.com requested deletion
    When the purge deadline passes
    Then the purge command for alice@example.com is sent again
    And no outcome is announced yet

  Scenario: The leaver's choices survive the retry
    Given security announced that alice@example.com requested deletion choosing memes=DELETE and comments=ANONYMIZE_AUTHOR
    When the purge deadline passes
    Then the purge command for alice@example.com is sent again
    And every purge command carries the choices memes=DELETE and comments=ANONYMIZE_AUTHOR

  Scenario: Silence outlasting every retry announces the failure, naming the partial purge
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    When the purge deadline passes and every retry is exhausted
    Then the portal announces the purge for alice@example.com failed
    And the failure names memes among the participants that already purged

  Scenario: A late confirmation cannot rewrite an announced failure
    Given security announced that alice@example.com requested deletion
    And the purge deadline passed and every retry was exhausted
    When every content service confirms its purge for alice@example.com echoing the purge command the portal gave up on
    Then the portal never announces the content of alice@example.com purged

  Scenario: A confirmation from a closed case does not touch a new one
    Given security announced that alice@example.com requested deletion
    And the purge deadline passed and every retry was exhausted
    And security announces another deletion request for alice@example.com
    When every content service confirms its purge for alice@example.com echoing the purge command the portal gave up on
    Then the portal never announces the content of alice@example.com purged

  Scenario: An outcome the portal failed to announce is announced at the next sweep
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    And comments confirmed its purge for alice@example.com
    And collections confirmed its purge for alice@example.com
    But the announcement never left the portal
    When the next sweep comes around
    Then the portal announces the content of alice@example.com purged

  Scenario: An outcome that reached security is not announced twice
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    And comments confirmed its purge for alice@example.com
    And collections confirmed its purge for alice@example.com
    And the announcement reached security
    When the next sweep comes around
    Then no outcome is announced again

  Scenario: The portal keeps no memory of finished cases beyond the retention period
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    And comments confirmed its purge for alice@example.com
    And collections confirmed its purge for alice@example.com
    And the announcement reached security
    When the retention period passes
    And security replays the deletion fact for alice@example.com
    Then a fresh purge command for alice@example.com opens a brand-new case

  Scenario: With no content participants the portal is instantly clean
    Given the portal has no content participants configured
    When security announces that alice@example.com requested deletion
    Then the portal announces the content of alice@example.com purged
