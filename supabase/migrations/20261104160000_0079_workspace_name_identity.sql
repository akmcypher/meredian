-- Workspace identity is its UUID. Human-readable names are not global identifiers.
-- Keeping a global name constraint rolls back confirmation for equal display names.
ALTER TABLE public.organizations DROP CONSTRAINT organizations_name_key;
