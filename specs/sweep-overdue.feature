Feature: The SWEEP — silence has a deadline

  Nobody is allowed to keep the leaver waiting forever. When a participant stays
  silent past the deadline, the SWEEP re-sends the PURGE command; when every retry
  is exhausted, the portal gives up honestly — it commands the RESTORE (the
  compensation that makes the apology true), and only then announces the failure,
  naming who had already purged. The SWEEP is also the safety net for the portal's
  own promises: an OUTCOME that never left is announced at the next round, and one
  that reached security is never announced twice.

  Rule: Silence past the deadline retries the PURGE command before giving up

    Example:
      Given security announced that alice@example.com requested deletion
      When the PURGE deadline passes
      Then the PURGE command for alice@example.com is sent again
      And no OUTCOME is announced yet

  Rule: The leaver's choices survive the retry

    Example:
      Given security announced that alice@example.com requested deletion choosing memes=DELETE and comments=ANONYMIZE_AUTHOR
      When the PURGE deadline passes
      Then the PURGE command for alice@example.com is sent again
      And every PURGE command carries the choices memes=DELETE and comments=ANONYMIZE_AUTHOR

  Rule: Silence outlasting every retry announces the failure, naming the partial purge

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      When the PURGE deadline passes and every retry is exhausted
      Then the portal announces the PURGE for alice@example.com failed
      And the failure names memes among the participants that already purged

  Rule: Giving up restores what it had reserved, and only then apologises

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      When the PURGE deadline passes and every retry is exhausted
      Then the portal commands the RESTORE of the content of alice@example.com
      And the RESTORE is commanded before the OUTCOME is announced
      And the portal announces the PURGE for alice@example.com failed

  Rule: An OUTCOME the portal failed to announce is announced at the next SWEEP

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      And collections confirmed its PURGE for alice@example.com
      But the announcement never left the portal
      When the next SWEEP comes around
      Then the portal announces the content of alice@example.com purged

  Rule: An OUTCOME that reached security is not announced twice

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      And collections confirmed its PURGE for alice@example.com
      And the announcement reached security
      When the next SWEEP comes around
      Then no OUTCOME is announced again
