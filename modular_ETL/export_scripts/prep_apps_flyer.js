db.apps_flyer.drop();
db.app_flier.find().forEach(function(doc){
    var newdoc = JSON.parse(doc.payload);
    db.apps_flyer.save(newdoc);
})
