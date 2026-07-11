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

  Scenario: A confirmation for nobody's saga is a stray, not an error
    When memes confirms its purge for nobody@example.com
    Then no outcome is announced yet

  Scenario: Silence past the deadline announces the failure
    Given security announced that alice@example.com requested deletion
    And memes confirmed its purge for alice@example.com
    When the purge deadline passes
    Then the portal announces the purge for alice@example.com failed

  Scenario: With no content participants the portal is instantly clean
    Given the portal has no content participants configured
    When security announces that alice@example.com requested deletion
    Then the portal announces the content of alice@example.com purged
