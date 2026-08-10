Feature: Beginning the offboarding — a deletion FACT opens a CASE

  The portal's process manager for account deletion, extracted from the identity
  service so identity stays reusable. Security announces the FACT that an account
  requested deletion; this service opens a CASE and commands every configured
  content participant to PURGE — reserve the leaver's content, ready to be erased
  for good or brought back. Commands are idempotent BY DEFAULT (workspace ADR 0006
  — enforced by the generic IdempotentCommandsTest, not restated per scenario);
  the scenarios pin the message choreography.

  Rule: A deletion FACT commands the content PURGE

    Example:
      When security announces that alice@example.com requested deletion
      Then a PURGE command for alice@example.com goes out to the content services

  Rule: The leaver's choices ride the command untouched

    Example:
      When security announces that alice@example.com requested deletion choosing memes=DELETE and comments=ANONYMIZE_AUTHOR
      Then the PURGE command carries the choices memes=DELETE and comments=ANONYMIZE_AUTHOR

  Rule: A second deletion request joins the CASE already underway

    Example:
      Given security announced that alice@example.com requested deletion
      When security announces another deletion request for alice@example.com
      Then the PURGE command for alice@example.com is sent again
      And no OUTCOME is announced yet

  Rule: A FACT the portal cannot place is dropped — never the requests behind it

    Example:
      When security announces a deletion request that names no account
      And security announces a deletion request with a garbled identity
      And security announces that alice@example.com requested deletion
      Then a PURGE command for alice@example.com goes out to the content services
      And no OUTCOME is announced yet

  Rule: With no content participants the portal is instantly clean

    Example:
      Given the portal has no content participants configured
      When security announces that alice@example.com requested deletion
      Then the portal announces the content of alice@example.com purged

  Rule: Past RETENTION the portal remembers nothing — a replayed FACT opens a brand-new CASE

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      And collections confirmed its PURGE for alice@example.com
      And the announcement reached security
      When the RETENTION period passes
      And security replays the deletion FACT for alice@example.com
      Then a fresh PURGE command for alice@example.com opens a brand-new CASE
