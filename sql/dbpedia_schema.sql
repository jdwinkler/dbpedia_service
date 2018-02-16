create schema dbpedia

create table dbpedia.subjects (
	
	subject_id	serial not null,
	name	text not null,
	
	constraint paper_pkey primary key (paper_id)
);

create index subject_idx on dbpedia.subjects (name);

create table dbpedia.predicate_values (

	subject_id integer not null,
	predicate text not null,
	value text not null

	CONSTRAINT fk_subject_id FOREIGN KEY ( subject_id ) REFERENCES dbpedia.subjects ( subject_id )

);

create index pv_subject_id_idx on dbpedia.predicate_values (subject_id);
