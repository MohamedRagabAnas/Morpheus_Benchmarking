-- generated by LdbcUtil on Wed Nov 06 15:16:16 EET 2019

SET SCHEMA warehouse.ldbc100

CREATE ELEMENT TYPE City ( name STRING, url STRING )
CREATE ELEMENT TYPE Company ( name STRING, url STRING )
CREATE ELEMENT TYPE Continent ( name STRING, url STRING )
CREATE ELEMENT TYPE Country ( name STRING, url STRING )
CREATE ELEMENT TYPE Forum ( title STRING, creationDate INTEGER )
CREATE ELEMENT TYPE Person ( id INTEGER, firstName STRING, lastName STRING, gender STRING, birthday INTEGER, birthday_day INTEGER, birthday_month INTEGER, creationDate INTEGER, locationIP STRING, browserUsed STRING, email STRING, speaks STRING )
CREATE ELEMENT TYPE Message (id INTEGER,creationDate INTEGER, locationIP STRING, browserUsed STRING, content STRING, length INTEGER )
CREATE ELEMENT TYPE Comment EXTENDS Message()
CREATE ELEMENT TYPE Post EXTENDS Message (imageFile STRING, language STRING)
CREATE ELEMENT TYPE Tag ( name STRING, url STRING )
CREATE ELEMENT TYPE Tagclass ( name STRING, url STRING )
CREATE ELEMENT TYPE University ( name STRING, url STRING )

CREATE ELEMENT TYPE CONTAINER_OF
CREATE ELEMENT TYPE HAS_CREATOR
CREATE ELEMENT TYPE HAS_INTEREST
CREATE ELEMENT TYPE HAS_MEMBER ( joinDate INTEGER )
CREATE ELEMENT TYPE HAS_MODERATOR
CREATE ELEMENT TYPE HAS_TAG
CREATE ELEMENT TYPE HAS_TYPE
CREATE ELEMENT TYPE IS_LOCATED_IN
CREATE ELEMENT TYPE IS_PART_OF
CREATE ELEMENT TYPE IS_SUBCLASS_OF
CREATE ELEMENT TYPE KNOWS ( creationDate INTEGER )
CREATE ELEMENT TYPE LIKES ( creationDate INTEGER )
CREATE ELEMENT TYPE REPLY_OF
CREATE ELEMENT TYPE STUDY_AT ( classYear INTEGER )
CREATE ELEMENT TYPE WORK_AT ( workFrom INTEGER )

CREATE GRAPH LDBC (
    -- Node types including mappings
        (Post) FROM post,
	(Tag) FROM tag,
	(Company) FROM company,
	(Tagclass) FROM tagclass,
	(Continent) FROM continent,
	(Person) FROM person,
	(Forum) FROM forum,
	(Comment) FROM comment,
	(University) FROM university,
	(Country) FROM country,
	(City) FROM city,

    -- Edge types including mappings
    (Country)-[IS_PART_OF]->(Continent)
		FROM country_ispartof_continent edge START NODES (Country) FROM country node JOIN ON edge.country.id = node.id END NODES (Continent) FROM continent node JOIN ON edge.continent.id = node.id,
	(Tagclass)-[IS_SUBCLASS_OF]->(Tagclass)
		FROM tagclass_issubclassof_tagclass edge START NODES (Tagclass) FROM tagclass node JOIN ON edge.tagclass.id0 = node.id END NODES (Tagclass) FROM tagclass node JOIN ON edge.tagclass.id1 = node.id,
	(Comment)-[REPLY_OF]->(Comment)
		FROM comment_replyof_comment edge START NODES (Comment) FROM comment node JOIN ON edge.comment.id0 = node.id END NODES (Comment) FROM comment node JOIN ON edge.comment.id1 = node.id,
	(Person)-[KNOWS]->(Person)
		FROM person_knows_person edge START NODES (Person) FROM person node JOIN ON edge.person.id0 = node.id END NODES (Person) FROM person node JOIN ON edge.person.id1 = node.id,
	(Forum)-[HAS_TAG]->(Tag)
		FROM forum_hastag_tag edge START NODES (Forum) FROM forum node JOIN ON edge.forum.id = node.id END NODES (Tag) FROM tag node JOIN ON edge.tag.id = node.id,
	(Person)-[WORK_AT]->(Company)
		FROM person_workat_company edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (Company) FROM company node JOIN ON edge.company.id = node.id,
	(Person)-[LIKES]->(Post)
		FROM person_likes_post edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (Post) FROM post node JOIN ON edge.post.id = node.id,
	(Person)-[HAS_INTEREST]->(Tag)
		FROM person_hasinterest_tag edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (Tag) FROM tag node JOIN ON edge.tag.id = node.id,
	(Company)-[IS_LOCATED_IN]->(Country)
		FROM company_islocatedin_country edge START NODES (Company) FROM company node JOIN ON edge.company.id = node.id END NODES (Country) FROM country node JOIN ON edge.country.id = node.id,
	(Comment)-[HAS_CREATOR]->(Person)
		FROM comment_hascreator_person edge START NODES (Comment) FROM comment node JOIN ON edge.comment.id = node.id END NODES (Person) FROM person node JOIN ON edge.person.id = node.id,
	(Post)-[HAS_TAG]->(Tag)
		FROM post_hastag_tag edge START NODES (Post) FROM post node JOIN ON edge.post.id = node.id END NODES (Tag) FROM tag node JOIN ON edge.tag.id = node.id,
	(Post)-[HAS_CREATOR]->(Person)
		FROM post_hascreator_person edge START NODES (Post) FROM post node JOIN ON edge.post.id = node.id END NODES (Person) FROM person node JOIN ON edge.person.id = node.id,
	(Comment)-[REPLY_OF]->(Post)
		FROM comment_replyof_post edge START NODES (Comment) FROM comment node JOIN ON edge.comment.id = node.id END NODES (Post) FROM post node JOIN ON edge.post.id = node.id,
	(Post)-[IS_LOCATED_IN]->(Country)
		FROM post_islocatedin_country edge START NODES (Post) FROM post node JOIN ON edge.post.id = node.id END NODES (Country) FROM country node JOIN ON edge.country.id = node.id,
	(Comment)-[IS_LOCATED_IN]->(Country)
		FROM comment_islocatedin_country edge START NODES (Comment) FROM comment node JOIN ON edge.comment.id = node.id END NODES (Country) FROM country node JOIN ON edge.country.id = node.id,
	(University)-[IS_LOCATED_IN]->(City)
		FROM university_islocatedin_city edge START NODES (University) FROM university node JOIN ON edge.university.id = node.id END NODES (City) FROM city node JOIN ON edge.city.id = node.id,
	(Forum)-[CONTAINER_OF]->(Post)
		FROM forum_containerof_post edge START NODES (Forum) FROM forum node JOIN ON edge.forum.id = node.id END NODES (Post) FROM post node JOIN ON edge.post.id = node.id,
	(Forum)-[HAS_MEMBER]->(Person)
		FROM forum_hasmember_person edge START NODES (Forum) FROM forum node JOIN ON edge.forum.id = node.id END NODES (Person) FROM person node JOIN ON edge.person.id = node.id,
	(Person)-[IS_LOCATED_IN]->(City)
		FROM person_islocatedin_city edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (City) FROM city node JOIN ON edge.city.id = node.id,
	(Person)-[STUDY_AT]->(University)
		FROM person_studyat_university edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (University) FROM university node JOIN ON edge.university.id = node.id,
	(Tag)-[HAS_TYPE]->(Tagclass)
		FROM tag_hastype_tagclass edge START NODES (Tag) FROM tag node JOIN ON edge.tag.id = node.id END NODES (Tagclass) FROM tagclass node JOIN ON edge.tagclass.id = node.id,
	(Forum)-[HAS_MODERATOR]->(Person)
		FROM forum_hasmoderator_person edge START NODES (Forum) FROM forum node JOIN ON edge.forum.id = node.id END NODES (Person) FROM person node JOIN ON edge.person.id = node.id,
	(Comment)-[HAS_TAG]->(Tag)
		FROM comment_hastag_tag edge START NODES (Comment) FROM comment node JOIN ON edge.comment.id = node.id END NODES (Tag) FROM tag node JOIN ON edge.tag.id = node.id,
	(Person)-[LIKES]->(Comment)
		FROM person_likes_comment edge START NODES (Person) FROM person node JOIN ON edge.person.id = node.id END NODES (Comment) FROM comment node JOIN ON edge.comment.id = node.id,
	(City)-[IS_PART_OF]->(Country)
		FROM city_ispartof_country edge START NODES (City) FROM city node JOIN ON edge.city.id = node.id END NODES (Country) FROM country node JOIN ON edge.country.id = node.id
)
