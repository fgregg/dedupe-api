(function(){
    get_cluster();
    function acceptAll(e){
        e.preventDefault();
        $('#group-display').spin('large');
        $.getJSON(mark_all_clusters_url, {'action': 'yes'}, function(resp){
            $('#group-display').spin(false);
            $('#group-display').html("<h3>" + resp['message'] + "</h3>")
            $('#review-buttons').hide()
            $('#counter').parent().hide()
        })
    }
    function markEntity(e){
        e.preventDefault();
        var entity_id = $('#group-table').data('entity_id');
        var match_ids = []
        var distinct_ids = []
        $.each($('input[type="checkbox"]'), function(i, inp){
            var record_id = $(inp).parent().data('record_id')
            if($(inp).is(':checked')){
                match_ids.push(record_id)
            } else {
               distinct_ids.push(record_id)
            }
        })
        var params = {
            'entity_id': entity_id,
            'match_ids': match_ids.join(','),
            'distinct_ids': distinct_ids.join(',')
        }
        console.log(params);
        $.getJSON(mark_cluster_url, params, function(resp){
            get_cluster();
        })
    }
    function get_cluster(){
        $('#group-display').spin('large');
        $.getJSON(get_cluster_url, {}, function(resp){
            $('#group-display').spin(false);
            if (resp.objects.length > 0){
                var template = new EJS({'text': $('#reviewTemplate').html()})
                $('#group-display').html(template.render({resp: resp}));
                $('.mark-entity').on('click', markEntity);
                $('.accept-all').on('click', acceptAll);
            } else {
                $('#group-display').html("<h3>You're done!</h3>")
                $('#review-buttons').hide()
                $('#counter').parent().hide()
            }
        })
    }
})();

