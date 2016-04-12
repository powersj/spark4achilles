/**
  * @author Joshua Powers <powersj@gatech.edu>
  */

package edu.gatech.cse8803.model


case class Group300Data(
                         provider_id: Int,
                         specialty_concept_id: String,
                         care_site_id: Int
                       )

case class Group400Data(
                         person_id: Int,
                         provider_id: String,
                         condition_concept_id: String,
                         condition_type_concept_id: String,
                         visit_occurrence_id: String,
                         condition_start_date: String,
                         condition_end_date: String
                       )

