# Security Policy

Axon accepts private vulnerability reports through GitHub Security Advisories. Do not open a public issue for suspected security problems.

Report anything that could:

- expose cloud credentials, signed URLs, or other secrets to the browser bundle
- weaken the trusted native control-plane boundary
- widen browser object access beyond the intended table, snapshot, or file scope
- leak data through fallback paths, logs, or error messages
- bypass origin, CORS, or access-policy expectations

Include, when possible:

- the affected crate, file, or runtime boundary
- a commit SHA, release tag, or branch name
- concise reproduction steps
- whether the issue affects native, browser, or control-plane code
- whether the impact is confidentiality, integrity, availability, or policy bypass

We triage reports privately and coordinate a fix before disclosure.
