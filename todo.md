# TODO — microservice-offboarding

Only open items. History = git log.

- **Kompensacja sagi (ADR 0007) — WDROŻONE 2026-08-08.** Orkiestrator wysyła trzy komendy:
  `PURGE_USER_CONTENT` (oznaczenie, odwracalne), `ERASE_USER_CONTENT` (domknięcie, kasuje) i
  `RESTORE_USER_CONTENT` (kompensacja). Otwarte po tej zmianie:
  - ~~`microservice-user-collections` nie jest uczestnikiem dwufazowym~~ — ZROBIONE tego samego
    dnia: wszyscy trzej uczestnicy oznaczają, domykają i kompensują tak samo.
  - (opc.) licznik komend domknięcia/kompensacji w `/metrics` — dziś widać tylko `compensated`.

- (opc.) cid w outbox-stylu: dziś outcome'y publikowane są po przejściu stanu (at-least-once,
  konsumenci idempotentni); prawdziwy outbox miałby sens dopiero przy twardszych wymaganiach.
- (opc.) metryki domenowe: licznik sag STARTED/COMPLETED/COMPENSATED w /metrics.
- (opc.) trace: outcome'y sweepera nie niosą traceparent (sweep nie ma rodzica) — do przemyślenia,
  czy łączyć je z trace'em żądania przez zapamiętany traceparent w wierszu sagi (jak V16 w security).
