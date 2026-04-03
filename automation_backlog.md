# Automation Backlog

## Lead-source no-answer follow-up sequence

Needed automation:
- Build a sequence for leads coming in from various lead sources that do not answer the phone.
- Only include leads that are not on market.

Sequence requirements:
- Skip trace the owner
- Send email
- Send SMS
- Send voicemail drop
- Send direct mail

Important note:
- Voicemail drop is a new capability and will need to be added as part of this sequence work.

Suggested guardrails to confirm when we build it:
- Which lead sources should trigger the sequence
- How long to wait after the missed / unanswered call before starting
- Suppression rules if the lead replies, answers later, or goes on market
- Compliance checks for SMS, voicemail, and mail steps
