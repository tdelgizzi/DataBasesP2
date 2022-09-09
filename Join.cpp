#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) {
    
    
    //create vector of buckets
    vector<Bucket> buckets;
    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1;i++){
        Bucket temp = Bucket(disk);
        buckets.push_back(temp);
    }
    
    
    //read in left_rel (or bigger rel if you change to that
    for (unsigned int i = left_rel.first; i < left_rel.second;i++){
        //use last mem page as a buffer to read in from disk
        mem->loadFromDisk(disk,i,(MEM_SIZE_IN_PAGE - 1));
        Page  temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
        //hash values into the buckets
        for (unsigned int j = 0; j < temp.size();j++){
            Record data = temp.get_record(j);
            data.partition_hash();
            mem->mem_page(data.partition_hash() % (MEM_SIZE_IN_PAGE - 1))->loadRecord(data);
            if (mem->mem_page(data.partition_hash() % (MEM_SIZE_IN_PAGE - 1))->full()){
                unsigned int id = mem->flushToDisk(disk, data.partition_hash() % (MEM_SIZE_IN_PAGE - 1));
                buckets[data.partition_hash() % (MEM_SIZE_IN_PAGE - 1)].add_left_rel_page(id);
                
            }//if
            
            
        }//for j
        
    }//for i
    
    for (unsigned int i = 0; i < (MEM_SIZE_IN_PAGE - 1); i++){
        if (mem->mem_page(i)->size() > 0){
            unsigned int id = mem->flushToDisk(disk, i);
            buckets[i].add_left_rel_page(id);
        }//if
    }//for
    
    
    
    //*****REPEAT FOR RIGHT RELATION******
    
    
    //read in right_rel (or bigger rel if you change to that
    for (unsigned int i = right_rel.first; i < right_rel.second;i++){
        //use last mem page as a buffer to read in from disk
        mem->loadFromDisk(disk,i,(MEM_SIZE_IN_PAGE - 1));
        Page  temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
        //hash values into the buckets
        for (unsigned int j = 0; j < temp.size();j++){
            Record data = temp.get_record(j);
            data.partition_hash();
            mem->mem_page(data.partition_hash() % (MEM_SIZE_IN_PAGE - 1))->loadRecord(data);
            if (mem->mem_page(data.partition_hash() % (MEM_SIZE_IN_PAGE - 1))->full()){
                unsigned int id = mem->flushToDisk(disk, data.partition_hash() % (MEM_SIZE_IN_PAGE - 1));
                buckets[data.partition_hash() % (MEM_SIZE_IN_PAGE - 1)].add_right_rel_page(id);
                
            }//if
            
            
        }//for j
        
    }//for i
    
    for (unsigned int i = 0; i < (MEM_SIZE_IN_PAGE - 1); i++){
        if (mem->mem_page(i)->size() > 0){
            unsigned int id = mem->flushToDisk(disk, i);
            buckets[i].add_right_rel_page(id);
        }//if
    }//for
    
    
    
    //return bucket vector
    return buckets;
    
    
}//partition

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    
    //create Vector of disk_page_id's for output
    vector<unsigned int> output;
    
    //use last mem slot as a buffer for the output;
    
    //read in first bucket
    //build hash table using first page
    //build rest of them using the rest of the pages -adding pairs if something is in it
    
    //loop through hash table and if there is something in the slot, flush it disk and add id to output vector
    
    //clear whole  hash table before next bucket
    
     for (unsigned int i = 0; i < partitions.size(); i++){
         vector<unsigned int> lids  = partitions[i].get_left_rel();
         vector<unsigned int> rids = partitions[i].get_right_rel();
         
         //if left is bigger than right
         if (partitions[i].num_left_rel_record > partitions[i].num_right_rel_record){
             
             //clear hash table
             for (unsigned int j = 0; j < MEM_SIZE_IN_PAGE-2;j++){
                 mem->mem_page(j)->reset();
             }//for j
             
             //build hash table using smaller releation
             for (unsigned int j = 0; j < rids.size();j++){
                 
                 //load first page into the last mem slot
                 mem->loadFromDisk(disk,rids[j],(MEM_SIZE_IN_PAGE - 1));
                 Page temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
                 
                 //
                 for (unsigned int k = 0; k < temp.size();k++){
                     Record data = temp.get_record(k);
                     //data.probe_hash();
                     //inseret data into hash table
                     mem->mem_page(data.probe_hash() % (MEM_SIZE_IN_PAGE - 2))->loadRecord(data);
                 }//for k
             }//for j end of building hash table with smaller relation
             
             //compare other releation and add to output buffer if matches
             for (unsigned int j  = 0; j < lids.size();j++){
                 mem->loadFromDisk(disk,lids[j],(MEM_SIZE_IN_PAGE - 1));
                 Page temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
                  for (unsigned int k = 0; k < temp.size();k++){
                      Record data = temp.get_record(k);
                      Page * tempPage = mem->mem_page(data.probe_hash() % (MEM_SIZE_IN_PAGE - 2));
                      for (unsigned int l = 0; l < tempPage->size(); l++){
                          if (data == tempPage->get_record(l)){
                              mem->mem_page(MEM_SIZE_IN_PAGE - 2)->loadPair(data, tempPage->get_record(l));
                              if(mem->mem_page(MEM_SIZE_IN_PAGE - 2)->full()){
                                  unsigned int id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2);
                                  output.push_back(id);
                              }
                          }//if
                      }//for l
                      
                  }//for k
                 
             }//for j end of comparisons
             
             
         }//if left rel > right rel
         else {
             //clear hash table
             for (unsigned int j = 0; j < MEM_SIZE_IN_PAGE-2;j++){
                 mem->mem_page(j)->reset();
             }//for j
             
             //build hash table using smaller releation
             for (unsigned int j = 0; j < lids.size();j++){
                 
                 //load first page into the last mem slot
                 mem->loadFromDisk(disk,lids[j],(MEM_SIZE_IN_PAGE - 1));
                 Page temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
                 
                 //
                 for (unsigned int k = 0; k < temp.size();k++){
                     Record data = temp.get_record(k);
                     //data.probe_hash();
                     //inseret data into hash table
                     mem->mem_page(data.probe_hash() % (MEM_SIZE_IN_PAGE - 2))->loadRecord(data);
                 }//for k
             }//for j end of building hash table with smaller relation
             
             //compare other releation and add to output buffer if matches
             for (unsigned int j  = 0; j < rids.size();j++){
                 mem->loadFromDisk(disk,rids[j],(MEM_SIZE_IN_PAGE - 1));
                 Page temp = Page(*mem->mem_page(MEM_SIZE_IN_PAGE - 1));
                 for (unsigned int k = 0; k < temp.size();k++){
                     Record data = temp.get_record(k);
                     Page * tempPage = mem->mem_page(data.probe_hash() % (MEM_SIZE_IN_PAGE - 2));
                     for (unsigned int l = 0; l < tempPage->size(); l++){
                         if (data == tempPage->get_record(l)){
                             mem->mem_page(MEM_SIZE_IN_PAGE - 2)->loadPair(data, tempPage->get_record(l));
                             if(mem->mem_page(MEM_SIZE_IN_PAGE - 2)->full()){
                                 unsigned int id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2);
                                 output.push_back(id);
                             }
                         }//if
                     }//for l
                     
                 }//for k
                 
             }//for j end of comparisons
             
             
             
             
         }//if right rel > left rel
         
         
         
         
     } //for i
    
    if(mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size() > 0){
        unsigned int id = mem->flushToDisk(disk, (MEM_SIZE_IN_PAGE - 2));
        output.push_back(id);
    }//if
    
    
    //return vector
    return output;
}//probe

