create or replace sequence auth_actors_id_seq;
create or replace table auth_actors (
	id int primary key default auth_actors_id_seq.nextval comment 'unique-id for an actor/requestor who is subject to protected access rules',
	type varchar(512) comment 'actor type - user_id or role_id (in future group_id)',
	name varchar(512) comment 'actor name - smadhavan or finance etc',
	created_at timestamp_ltz default current_timestamp() comment 'record created at',
	updated_at timestamp_ltz default current_timestamp() comment 'record updated at'
);

create or replace sequence auth_policies_id_seq;
create or replace table auth_policies (
	id int primary key default auth_policies_id_seq.nextval comment 'unique-id for a policy definition, has one-or-more associated rules',
	name varchar(512) comment 'policy name/title',
	description varchar(1024) comment 'policy description',
	is_enabled boolean default True comment 'policy state - enabled/disabled',
	created_at timestamp_ltz default current_timestamp() comment 'record created at',
	updated_at timestamp_ltz default current_timestamp() comment 'record updated at'
);

create or replace sequence auth_rules_id_seq;
create or replace table auth_rules (
	id int primary key default auth_rules_id_seq.nextval comment 'unique-id for a rule definition',
	schema_name varchar(512) comment 'schema_name of protected table',
	table_name varchar(512) comment 'table_name of protected table',
	column_name varchar(512) comment 'table.column to be protected',
	data_type varchar(64) comment 'pii or phi etc',
	column_mask_type varchar(512) comment 'type of masking - XXX, HASH, LAST4 etc, applicable for col masking',
	filter_exp  varchar(2048) comment 'string representing a valid where-clause expression, applicable for row filtering',
	description varchar(512) comment 'rule description',
	created_at timestamp_ltz default current_timestamp() comment 'record created at',
	updated_at timestamp_ltz default current_timestamp() comment 'record updated at'
);

create or replace table auth_actors_policies (
	actor_id int,
	policy_id int,
	foreign key(actor_id) references auth_actors(id),
	foreign key(policy_id) references auth_policies(id)
);

create or replace table auth_policies_rules (
	policy_id int,
	rule_id int,
	foreign key(policy_id) references auth_policies(id),
	foreign key(rule_id) references auth_rules(id)
);
