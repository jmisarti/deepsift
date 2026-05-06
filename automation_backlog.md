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

## New lead call-attempt tracking

Needed automation / agent function:
- Build a system for tracking call attempts for new leads.
- Track how many call touches have happened per lead and per phone number.
- Track final / blocking number outcomes such as `DNC`, `dead`, `wrong number`, and similar dispositions.
- Make the tracking usable for follow-up logic so we can tell when a lead has been worked enough or should be routed differently.

Important note:
- This likely belongs as an agent-style function rather than a simple sequence step.
- We want to come back to this next week.

Suggested design questions for later:
- Whether call attempts should be tracked at the lead level, person level, touchpoint level, or all three
- Which dispositions should stop future dialing automatically
- How manual call outcomes and automated call outcomes should be merged
- What touch thresholds should trigger escalation, pause, or channel switching

## Clever webhook follow-up

Important note:
- Clever replay payloads are now reaching the capture endpoint, but normal ongoing connection/deal updates do not appear to be posting yet.
- We need to revisit this after the support call and confirm which Clever events are actually emitted beyond `replay`.
- The webhook payload is missing the richer property-detail fields we used to get from the Google Sheet, and those details are important for downstream handling.

Follow-up questions for later:
- Whether Clever can send stage-change updates automatically or only manual replays
- Whether there is a separate event type or subscription toggle for ongoing updates
- Whether payload shape changes between `replay` and live update events
- Whether Clever can include richer seller/property fields comparable to the old worksheet data

## RentCast valuation placeholder

Needed integration placeholder:
- Add a future call to RentCast property valuation for valuation assistance.
- Endpoint to revisit: `https://developers.rentcast.io/reference/property-valuation`

Important note:
- This is only a placeholder for now; we have not designed how the valuation should be used in lead scoring, underwriting, offer guidance, or UI display yet.

Questions for later:
- When valuation should be triggered: on intake, on demand, or on a scheduled refresh
- Which address normalization rules we want before calling the valuation endpoint
- Whether we should store the raw valuation payload, normalized summary fields, or both
- Which workflows should consume the valuation once it is available

## REISift-only SMS / phone-context attachment

Future consideration:
- When SmrtPhone or similar inbound communication resolves only to REISift context, try to attach the new phone number to an existing local DeepSift property before treating it as unresolved.

Suggested resolution order:
- Match by existing local phone touchpoint first
- Then by REISift property UUID already linked on a local property
- Then by normalized address match
- Then by a single clear owner/person match
- Only create a brand-new local property if no existing local match exists

Important caution:
- A new phone number does not necessarily mean a new property.
- Existing leads may text or call from alternate numbers, and REISift may also contain owner numbers that DeepSift has not learned yet.
- We should prefer attaching those numbers to the existing lead/property record instead of creating duplicates.
