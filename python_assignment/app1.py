from flask import Flask, render_template, request, flash
import boto3
from botocore.exceptions import ClientError
from decouple import config

app = Flask(__name__)
app.config['SECRET_KEY']=config('aws_secret_access_key')
app.config['Access_KEY']=config('aws_access_key')

s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')


######################Home Page###########################
@app.route('/')
def home_page():
    return render_template('index.html')


######################List S3 buckets#######################
@app.route('/list')
def list_buckets():
    listOfBuckets = []
    for bucket in s3_resource.buckets.all():
        listOfBuckets.append(bucket.name)
    if len(listOfBuckets) == 0:
        flash('No Buckets!')
    return render_template('list.html',listOfBuckets=listOfBuckets)


########################Create/delete bucket######################
@app.route('/bucket')
def bucket_form():
    return render_template('bucket.html')

#Create Bucket
@app.route('/createBucket',methods=['POST'])
def create_bucket():
    bucketName = request.form.get('bucketName')
    try:
        s3.create_bucket(
            ACL='private',
            Bucket=bucketName,
            CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
        )
        flash('Bucket is created successfully')
    except:
        flash("Bucket already exists with this name!")
    return render_template('bucket.html')

#Delete Bucket
@app.route('/delBucket',methods=['POST'])
def delete_bucket():
    bucketName = request.form.get('bucketName')
    try:
        s3.delete_bucket(
            Bucket= bucketName
        )
        flash('Bucket is deleted successfully')
    except ClientError:
        flash('This bucket is either not empty or not exists!')
    return render_template('bucket.html')


#####################Create/delete folder#####################
@app.route('/folder')
def folder_form():
    return render_template('folder.html')

#Create folder
@app.route('/create',methods=['POST'])
def create_folder():
    bucketName = request.form.get('bucketName')
    folderName = request.form.get('folderName')+'/'
    try:
        bucket = s3_resource.Bucket(bucketName)
        bucket.put_object(Key=folderName)
        for obj in bucket.objects.all():
            if obj.key == folderName:
                flash('Folder is created successfully!')
    except:
        flash('No such Bucket!')
    return render_template('folder.html')

#Delete Folder
@app.route('/delete',methods=['POST'])
def delete_folder():
    bucketName = request.form.get('bucketName')
    folderName = request.form.get('folderName') + '/'
    try:
        objects = s3.list_objects(Bucket=bucketName, Prefix=folderName)
        files_in_folder = objects["Contents"]
        files_to_delete = []
        for f in files_in_folder:
            files_to_delete.append({"Key": f["Key"]})
        s3.delete_objects(
            Bucket=bucketName, Delete={"Objects": files_to_delete}
        )
        flash('Folder is deleted successfully!')
    except ClientError:
        flash('No such bucket!')
    except KeyError:
        flash('No such folder!')
    return render_template('folder.html')


################Upload file to S3#####################
@app.route('/upload')
def form():
    return render_template('upload.html')

@app.route('/upload', methods=["POST"])
def upload():
    bucketName = request.form.get('bucket')
    file = request.files['file']
    try:
        fileName = file.filename
        s3.upload_fileobj(file,bucketName,fileName)
        flash('File uploaded')
    except ClientError:
        flash('No such bucket!')
    return render_template('upload.html')


################Delete file from S3##################
@app.route('/delfile')
def del_file_form():
    return render_template('delfile.html')

@app.route('/delfile',methods=['POST'])
def delFile():
    bucketName = request.form.get('bucketName') 
    fileName = request.form.get('fileName')
    try:
        s3.delete_object(Bucket=bucketName, Key=fileName)
        flash('File is deleted')
    except ClientError:
        flash('No such bucket!!')
    return render_template('delfile.html')


##############Copy/Move file within S3##################
@app.route('/movefile')
def move():
    return render_template('movefile.html')

#Copy file within s3
@app.route('/copy',methods=['POST'])
def copy_s3_objects():
    sourceBucket = request.form.get('sourceBucket')
    sourceFile = request.form.get('sourceFile')
    destBucket = request.form.get('destBucket')
    
    try:
        copy_source = {
            'Bucket': sourceBucket,
            'Key': sourceFile
        }
        flash('File copied')
        s3_resource.meta.client.copy(copy_source, destBucket, sourceFile)
    except ClientError:
        flash('You have entered wrong details!!')
    return render_template('movefile.html')

#Move file within s3
@app.route('/move',methods=['POST'])
def move_files():
    sourceBucket = request.form.get('sourceBucket')
    sourceFile = request.form.get('sourceFile')
    destBucket = request.form.get('destBucket')

    try:
        copy_source = {
            'Bucket':sourceBucket,
            'Key':sourceFile
        }
        s3_resource.meta.client.copy(copy_source, destBucket,sourceFile)
        s3.delete_object(
            Bucket=sourceBucket,
            Key=sourceFile
        )
        flash('File moved')
    except ClientError:
        flash('You have entered wrong details!!')
    return render_template('movefile.html')


if __name__ == "__main__":
    app.run(debug=True)