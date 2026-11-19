-- Why a pipeline's desired state is what it is: '' = chosen in the web (Start/Stop), 'observed_stop' = the
-- active web saw the pipeline stop cleanly without a web action (`strand stop`, SIGTERM from an operator) and
-- recorded desired=stopped, so it is not autostarted on the next web start. A crash keeps desired=running.
ALTER TABLE pipeline_desired ADD COLUMN reason text NOT NULL DEFAULT '';
