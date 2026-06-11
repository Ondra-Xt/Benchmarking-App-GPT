# ACO ShowerDrain M+ conditional scoring and customer-presentation policy

## Decision scope

This policy applies to the four ACO ShowerDrain M+ compound assemblies modeled as
`channel_body_x_drain_body_x_grate`. It is a review policy only and does not approve
or implement customer-facing publication.

## Canonical/default context

- M+ flow remains unresolved when no scoring scenario is selected.
- `flow_rate_lps` and `selected_default_flow_rate_lps` remain empty.
- Canonical `ready_for_benchmark` and `ready_for_customer_view` remain false.
- The blocked reason must state that conditional-parameter selection is required.
- No average, maximum-only, minimum-only, preferred, or hidden scalar default may
  replace the two source-backed values.

## Explicit benchmark scenarios

- The explicit **10 mm head-water-level** scenario resolves to **0.40 l/s** from
  `Conditional_Technical_Values` and may be benchmark-scored.
- The explicit **20 mm head-water-level** scenario resolves to **0.46 l/s** from
  `Conditional_Technical_Values` and may be benchmark-scored.
- The condition label, type, value, and unit remain visible in scenario output.
- Scenario benchmark readiness does not promote canonical/default benchmark or
  customer readiness.

## Customer presentation

M+ is scoreable only after explicit condition selection. Any later customer-facing
implementation must require an explicit scenario/condition selection and display the
selected flow and its condition together. It must never present 0.40 l/s or 0.46 l/s
as an unconditional product property, and it must not introduce a hidden default.

The current policy gate is `requires_manual_customer_policy_approval`. A later,
separate approval branch may implement conditional customer presentation. Until that
approval, canonical default rows remain blocked and customer view remains disabled.
