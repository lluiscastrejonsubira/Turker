#!/usr/bin/python

import api
import os
from database import *
import settings
import argparse
import random
import math
import traceback
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("turker")

def _recursive_images(path, session, group_name=None):
    filenames = os.listdir(path)
    
    for filename in filenames:
        filename = os.path.join(path, filename)
        if os.path.isdir(filename):
            _recursive_images(filename, session, group_name)
        elif filename.split(os.path.sep)[-1][-4:] == '.jpg' :
            id = int(filename.split(os.path.sep)[-1][-16:-4])
            image = Image(id=id, path='/' + filename, group=group_name)
            session.add(image)
            logger.info("Image %s" % (filename, ))
    
    session.commit()
    logger.info("Images inserted into the database")
    
    
def create_HIT_batch(server, session, batch):
    
    # Load external question
    name = batch.get('Batch', 'name')
    sandbox = batch.getboolean('Batch', 'sandbox')
    title = batch.get('Batch', 'title')
    description = batch.get('Batch', 'description')
    question = batch.get('Batch', 'question')
    amount = batch.getfloat('Batch', 'amount')
    duration = batch.getint('Batch', 'duration')
    lifetime = batch.getint('Batch', 'lifetime')
    keywords = batch.get('Batch', "keywords")
    auto_approve = batch.getint('Batch', 'auto_approve')
    max_assigs = batch.getint('Batch', 'max_assigs')
    images_per_task = batch.getint('Batch', 'images_per_task')
    height = batch.getint('Batch', 'height')
    number_of_images = batch.getint('Batch', 'number_of_images')
    min_approved_percent = batch.get('Batch', 'min_approved_percent')
    min_approved_percent = None if min_approved_percent == 'None' \
                                else int(min_approved_percent)
    
    min_approved_amount = batch.get('Batch', 'min_approved_amount')
    min_approved_amount = None if min_approved_amount == 'None' \
                                else int(min_approved_amount)
    
    country_code = batch.get('Batch', 'country_code')
    country_code = None if country_code == 'None' else country_code
    
    image_group = batch.get('Batch', 'image_group')
    image_group = None if image_group == 'None' else image_group
    
    # Retrieve images
    images = session.query(Image)
    
    if image_group:
        images = images.filter(Image.group == image_group)
    
    images = images.all()
    
    # Sort images
    images = sorted(images, key= lambda image: image.id)
    
    # Select only number_of_images if specified
    if number_of_images != 0:
        aux = random.sample(images, number_of_images)
        print aux
        images = aux
        
    # Create random order for images
    random.shuffle(images)
        
    assert (len(images) % images_per_task) == 0, """Number of images
         in database must be a multiple of %d, instead 
         found %d images.""" % (images_per_task, len(images))
    
    # Create batch
    db_batch = Batch(name=name,
                              sandbox=sandbox,
                              title=title,
                              description=description,
                              question=question,
                              amount=amount,
                              duration=duration,
                              lifetime=lifetime,
                              keywords=keywords,
                              auto_approve=auto_approve,
                              max_assigs=max_assigs,
                              height=height,
                              number_of_images=number_of_images,
                              min_approved_percent=min_approved_percent,
                              min_approved_amount=min_approved_amount,
                              country_code=country_code
                             )
    session.add(db_batch)
    session.flush()
    
    for i in range(0, len(images), images_per_task):
        
        # Gather 3 images and their ids
        hit_images = [images[i + j] for j in range(images_per_task)]
        
        # Load HIT into the turk
        hit = server.createhit(title=title,
                        description=description,
                        page=question,
                        amount=amount,
                        duration=duration,
                        lifetime=lifetime,
                        maxAssigs=max_assigs,
                        height=height,
                        keywords=keywords,
                        autoapprove=auto_approve,
                        minapprovedpercent=min_approved_percent,
                        minapprovedamount=min_approved_amount,
                        countrycode=country_code)
         
        hitId = hit.values['hitid']
        typeId = hit.values['hittypeid']
        
        # Update db
        hit = HIT(hitId=hitId, 
                  typeId=typeId,
                  batchId=db_batch.id)
        session.add(hit)
        session.flush()
        
        for image in hit_images:
            entry = HIT_Image(imageId=image.id, hitId=hit.id)
            logger.debug(entry)
            session.add(entry)
        
        print "HIT %s created" % (hitId, )
       

def create_validation_HIT_batch(server, session, batch):
    
    # Load external question
    name = batch.get('Batch', 'name')
    sandbox = batch.getboolean('Batch', 'sandbox')
    title = batch.get('Batch', 'title')
    description = batch.get('Batch', 'description')
    question = batch.get('Batch', 'question')
    amount = batch.getfloat('Batch', 'amount')
    duration = batch.getint('Batch', 'duration')
    lifetime = batch.getint('Batch', 'lifetime')
    keywords = batch.get('Batch', "keywords")
    auto_approve = batch.getint('Batch', 'auto_approve')
    max_assigs = batch.getint('Batch', 'max_assigs')
    validations_per_task = batch.getint('Batch', 'validations_per_task')
    height = batch.getint('Batch', 'height')
    number_of_tasks = batch.get('Batch', 'number_of_tasks')
    number_of_tasks = None if number_of_tasks == 'None' \
                            else int(number_of_tasks)
    min_approved_percent = batch.get('Batch', 'min_approved_percent')
    min_approved_percent = None if min_approved_percent == 'None' \
                                else int(min_approved_percent)
    
    min_approved_amount = batch.get('Batch', 'min_approved_amount')
    min_approved_amount = None if min_approved_amount == 'None' \
                                else int(min_approved_amount)
    
    country_code = batch.get('Batch', 'country_code')
    country_code = None if country_code == 'None' else country_code
    
    batch_tasks = batch.get('Batch', 'batch_tasks')
    
    # Retrieve hits in batch to validate
    batch_to_validate = session.query(Batch).\
                                filter(Batch.name == batch_tasks).\
                                one()
    hit_ids = [i.id for i in batch_to_validate.hits]
    
    # Retrieve tasks to validate 
    tasks_to_validate = session.query(Task).\
                join(Assignment, Assignment.id == Task.assignmentId).\
                filter(Assignment.hitId.in_(hit_ids),
                       Task.submitted == True).\
                all()
                
    # Sort tasks
    tasks_to_validate = sorted(tasks_to_validate, key= lambda task: task.id)
    
    # Select only number_of_tasks if specified
    if number_of_tasks != None:
        aux = random.sample(tasks_to_validate, number_of_tasks)
        print aux
        tasks_to_validate = aux
        
    # Create random order for images
    random.shuffle(tasks_to_validate)
        
    assert (len(tasks_to_validate) % validations_per_task) == 0, """Number of images
         in database must be a multiple of %d, instead 
         found %d images.""" % (validations_per_task, len(tasks_to_validate))
    
    # Create batch
    db_batch = Batch(name=name,
                              sandbox=sandbox,
                              title=title,
                              description=description,
                              question=question,
                              amount=amount,
                              duration=duration,
                              lifetime=lifetime,
                              keywords=keywords,
                              auto_approve=auto_approve,
                              max_assigs=max_assigs,
                              height=height,
                              min_approved_percent=min_approved_percent,
                              min_approved_amount=min_approved_amount,
                              country_code=country_code
                             )
    session.add(db_batch)
    session.flush()
    
    for i in range(0, len(tasks_to_validate), validations_per_task):
        
        # Gather validations_per_task tasks 
        hit_images = [tasks_to_validate[i + j] for j in range(validations_per_task)]
        
        # Load HIT into the turk
        hit = server.createhit(title=title,
                        description=description,
                        page=question,
                        amount=amount,
                        duration=duration,
                        lifetime=lifetime,
                        maxAssigs=max_assigs,
                        height=height,
                        keywords=keywords,
                        autoapprove=auto_approve,
                        minapprovedpercent=min_approved_percent,
                        minapprovedamount=min_approved_amount,
                        countrycode=country_code)
         
        hitId = hit.values['hitid']
        typeId = hit.values['hittypeid']
        
        # Update db
        hit = HIT(hitId=hitId, 
                  typeId=typeId,
                  batchId=db_batch.id)
        session.add(hit)
        session.flush()
        
        for task in hit_images:
            entry = HIT_Image(hitId=hit.id, validatedTaskId = task.id)
            logger.debug(entry)
            session.add(entry)
        
        print "HIT %s created" % (hitId, ) 

def approve_workers(server, session, batch_name):
    '''
    Approves assignments and pays workers
    '''
    # Put here your code to pay workers with bonuses     
    
def delete_all_HITs(server, session):
    
    # Purge all hits
    server.purge()
    print "All hits deleted"
     
     
def delete_batch(server, session, batch_name):

    # Delete results for this batch
    print batch_name
    batch = session.query(Batch).\
            filter(Batch.name == batch_name).\
            one()                
    
    for hit in batch.hits:
        for assig in hit.assignments:
            for task in assig.tasks:
                for result in task.results:
                        session.delete(result)
                session.delete(task)
            session.delete(assig)
   
        hit_images = session.query(HIT_Image).\
                        filter(HIT_Image.hitId == hit.id).\
                        all()
        
        for hi in hit_images:
            bow_items = session.query(Bow).\
                        filter(Bow.hit_imageId == hi.id).\
                        all()
            
            for item in bow_items:
                session.delete(item)
            
            session.flush() 
            session.delete(hi)
       
        server.disable(hit.hitId)
        session.delete(hit)
   
    session.delete(batch)
    
def expire_batch(server, session, batch_name):
    batches = session.query(Batch).\
                    filter(Batch.name == batch_name).\
                    all()
    
    hits = [i for batch in batches for i in batch.hits]  
    
    for hit in batch.hits:
        server.expire(hit.hitId)
    

def extend_batch(server, session, batch_name, time_in_seconds):
    batch = session.query(Batch).\
                    filter(Batch.name == batch_name).\
                    one()
                    
    for hit in batch.hits:
        server.extend(hit.hitId, time_in_seconds)

def _create_server(config, batch):
    
    access_key = config.get('Settings', 'access_key')
    secret_key = config.get('Settings', 'secret_key')
    server_url = config.get('Settings', 'server_url')
    sandbox = batch.getboolean('Batch', 'sandbox')
    server = api.Server(secret_key, access_key, server_url, sandbox)
    
    return server

 
if __name__ == "__main__":
        
    # Start random object
    random.seed()
    
    # Create argument parser and parse args
    parser = argparse.ArgumentParser(description="Manage the turk")
    parser.add_argument('operation', type=str, help='operation to be performed', 
                        choices=['new_config',
                                 'new_batch',
                                 'setup',
                                 'create_batch',
                                 'delete_batch',
                                 'approve',
                                 'expire',
                                 'extend',
                                 'add_image_group'
                                 ])
    parser.add_argument('-b', type=str, help="batch file", required=False)
    parser.add_argument('-o', type=int, help="additional argument", required=False)
    parser.add_argument('-d', type=str, help="additional argument", required=False)
    args = parser.parse_args()

    if args.operation == "new_config":
        logger.info("Creating new config file")
        settings.create_default_settings()
        logger.info("New config file created")
    
    elif args.operation == 'new_batch':
        
        if args.b is None:
            print "Specify a path to create the batch with option -o"
            exit()
            
        logger.info("Creating new batch")
        settings.create_new_batch(args.b)
        logger.info("New batch created at %s" % (args.b, ))
    
    elif args.operation == 'setup':
        
        logger.info("Setting up turker")
        setup()
        config = settings.get_settings()
        with connect() as session:
            images_path = config.get('Settings', 'images_path')
            _recursive_images(images_path, session)
            insert_instructions_into_db(session)
        
        logger.info("Turker set up")
        
    elif args.operation == 'create_batch':
        
        if args.b is None:
            print "Specify batch file path with option -b"
            exit()
        
        logger.info('Creating batch of hits')
        config = settings.get_settings()
        batch = settings.get_batch_settings(args.b)
        server = _create_server(config, batch)
        
        with connect() as session:
            create_HIT_batch(server,
                             session,
                             batch=batch)
        logger.info('Batch of hits created') 
                    
    elif args.operation == 'delete_batch':
        if args.b is None:
            print "Specify batch file path with option -b"
            exit()
        
        logger.info('Deleting batch')
        config = settings.get_settings()
        batch = settings.get_batch_settings(args.b)
        server = _create_server(config, batch)
        with connect() as session:
            delete_batch(server, session, batch.get('Batch', 'name'))
        logger.info('Batch deleted!')
    
    elif args.operation == 'approve':
        
        if args.b is None:
            print "Specify batch file path with option -b"
            exit()
        
        config = settings.get_settings()
        batch = settings.get_batch_settings(args.b)
        server = _create_server(config, batch)
        logger.info('Approving workers')
        with connect() as session:
            approve_workers(server, session, batch.get('Batch', 'name'))
        logger.info('Workers approved')
        
    elif args.operation == 'expire':
        
        if args.b is None:
            print "Specify batch file path with option -b"
            exit()
        
        config = settings.get_settings()
        batch = settings.get_batch_settings(args.b)
        server = _create_server(config, batch)
        logger.info('Disabling hits')
        with connect() as session:
            expire_batch(server, session, batch.get('Batch', 'name'))
        logger.info('Hits disabled')
    
    elif args.operation == 'extend':
    
        if args.b is None:
            print "Specify batch file path with option -b"
            exit()
        
        if args.o is None:
            print "Specify time in seconds with option -o" 
        
        config = settings.get_settings()
        batch = settings.get_batch_settings(args.b)
        server = _create_server(config, batch)
        logger.info('Extending hits by %d seconds' % (args.o, ))
        with connect() as session:
            extend_batch(server,
                         session,
                         batch.get('Batch', 'name'),
                         args.o)
        logger.info('Hits extended')
        
        
    elif args.operation == 'add_image_group':
        
        if args.b is None:
            print 'Specify group name with option -b'
            exit()
        
        if args.d is None:
            print 'Specify image directory with option -d'
        
        with connect() as session:
            _recursive_images(args.d,
                              session,
                              group_name=args.b
                              )
