# TODO — microservice-offboarding

Only open items. History = git log.

- (opc.) cid w outbox-stylu: dziś outcome'y publikowane są po przejściu stanu (at-least-once,
  konsumenci idempotentni); prawdziwy outbox miałby sens dopiero przy twardszych wymaganiach.
- (opc.) metryki domenowe: licznik sag STARTED/COMPLETED/COMPENSATED w /metrics.
- (opc.) trace: outcome'y sweepera nie niosą traceparent (sweep nie ma rodzica) — do przemyślenia,
  czy łączyć je z trace'em żądania przez zapamiętany traceparent w wierszu sagi (jak V16 w security).
