Feature: Collecting the CONFIRMATIONS — the last one settles the CASE

  Each content participant answers the PURGE with a CONFIRMATION. One early answer
  settles nothing; the last one does two things in strict order: commands the
  ERASURE — the closure, the point of no return — and only then announces the
  single OUTCOME security waits for. A CONFIRMATION that fits no open CASE — a
  stray, or a late echo of a CASE the portal already gave up on — changes nothing:
  an announced OUTCOME is never rewritten.

  Rule: An early CONFIRMATION announces nothing yet

    Example:
      Given security announced that alice@example.com requested deletion
      When memes confirms its PURGE for alice@example.com
      Then no OUTCOME is announced yet

  Rule: The last CONFIRMATION announces the portal purged

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      When collections confirms its PURGE for alice@example.com
      Then the portal announces the content of alice@example.com purged

  Rule: The last CONFIRMATION also commands the ERASURE — that is what closes the CASE

    Example:
      Given security announced that alice@example.com requested deletion choosing memes=DELETE and comments=ANONYMIZE_AUTHOR
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      When collections confirms its PURGE for alice@example.com
      Then the portal commands the ERASURE of the content of alice@example.com
      And the ERASURE command carries the choices memes=DELETE and comments=ANONYMIZE_AUTHOR
      And the ERASURE is commanded before the OUTCOME is announced

  Rule: A CONFIRMATION may echo the PURGE command it answers

    Example:
      Given security announced that alice@example.com requested deletion
      And memes confirmed its PURGE for alice@example.com
      And comments confirmed its PURGE for alice@example.com
      When collections confirms its PURGE for alice@example.com echoing the PURGE command
      Then the portal announces the content of alice@example.com purged

  Rule: A CONFIRMATION for nobody's CASE is a stray, not an error

    Example:
      When memes confirms its PURGE for nobody@example.com
      Then no OUTCOME is announced yet

  Rule: A late CONFIRMATION cannot rewrite an announced failure

    Example:
      Given security announced that alice@example.com requested deletion
      And the PURGE deadline passed and every retry was exhausted
      When every content service confirms its PURGE for alice@example.com echoing the PURGE command the portal gave up on
      Then the portal never announces the content of alice@example.com purged

  Rule: A CONFIRMATION from a closed CASE does not touch a new one

    Example:
      Given security announced that alice@example.com requested deletion
      And the PURGE deadline passed and every retry was exhausted
      And security announces another deletion request for alice@example.com
      When every content service confirms its PURGE for alice@example.com echoing the PURGE command the portal gave up on
      Then the portal never announces the content of alice@example.com purged
