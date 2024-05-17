object Main {
  /**
    * Author: Aaron M Ogle
    * Description: 
      
    * Schema changes can create challenges for ETL processes so it is crucial to detect them
    * as quickly as possible.  In this example program, with the help of chatgpt, we create methods 
    * to detect changes in a schema.
    * 
    * The goal of this program is to become more familiar with the Scala language, and to create
    * a basic way to check if changes were made in a sample schema.  Detection of schema changes can
    * help with being proactive with how these changes will impact downstream processes - the
    * change of a schema will impact ETL processes so it is important to be notified of any 
    * changes in a schema as quickly as possible.
    * 
    * To run the script, make sure that the Scala compiler (scalac) is installed on your system. 
    * 
    * Once installed, you can execute the script by navigating to where the .scala file is stored
    * on your machine and running the below command:
    * 
    * scala ProgramName.scala
    * 
    * Example:
    * 
    * scala DetectSchemaChanges.scala
    * Detected changes in schema: 
    * Added Fields: email
    * Removed Fields: name
    */

  // Define a case class to represent a schema
  /* 
    Case classes in Scala are special types of classes that come with several built in features
    such as immutability and automatic methods (equals, toString, etc)
   */
  case class Schema(
    // Name of the field in the schema
    fieldName: String, 
    
    // Data type of the field in the schema
    fieldType: String
 )
  /**
    * Parse schema information from a list of strings and extract the schema objects
    * 
    * @param schemaInfo - list of strings containing schema information 
    * @return - list of schema objects parsed from the input strings
    */
  def parseSchema(schemaInfo: List[String]): List[Schema] = {
    // Use the map function on the schemaInfo list - we use info as a variable
    // so that we can apply the split function to each string in the list
    schemaInfo.map {info => 
      val parts = info.split(",")
      // Create a schema object using the parts obtained from splitting the info
      // variable string - we assume with this that each string will have a 
      // field name and field type
      Schema(parts(0), parts(1))  
    }
  }

  /**
    * Compare two schema lists to detect if there are any changes between them.
    *
    * @param oldSchema - The old version of the schema.
    * @param newSchema - The new version of the schema.
    * @return - A list of strings describing detected changes.
    * 
    * filterNot - method in Scala that is used to filter elements of a collection
    *  based on a predicate function that returns a boolean value
    * 
    * contains - method in Scala that checks whether a collection contains a specific element
    */
  def detectSchemaChanges(oldSchema: List[Schema], newSchema: List[Schema]): List[String] = {
    // Find new fields that have been added to the schema
    val addedFields = newSchema.filterNot(oldSchema.contains).map(schema => s"Added Fields: ${schema.fieldName}")

    // Find fields that have been removed from the schema
    val removedFields = oldSchema.filterNot(newSchema.contains).map(schema => s"Removed Fields: ${schema.fieldName}")

    // Combine the addedFields and removedFields lists
    addedFields ++ removedFields
  }

  def main(args: Array[String]): Unit = {
    // Example input schemas
    val oldSchemaData = List("id, int", "name, string", "age, int")
    val newSchemaData = List("id, int", "email, string", "age, int")

    // Parse the schema information into a list of schema objects
    val oldSchema = parseSchema(oldSchemaData)
    val newSchema = parseSchema(newSchemaData)

    // Detect schema changes
    val changes = detectSchemaChanges(oldSchema, newSchema)

    // Display changes
    if (changes.nonEmpty) {
      println("Detected changes in schema: ")
      // Print the changes
      changes.foreach(println)
    } else {
      println("No schema changes detected.")
    }
  }
}