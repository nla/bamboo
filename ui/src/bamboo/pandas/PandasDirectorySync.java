package bamboo.pandas;

import java.util.*;

import static bamboo.pandas.PandasType.AGENCY;
import static bamboo.pandas.PandasType.COLLECTION;
import static bamboo.pandas.PandasType.SUBJECT;
import static java.util.stream.Collectors.toSet;

public class PandasDirectorySync {
//    PandasDAO pandasDAO;
//    Categories categories;
//    Agencies agencies;
//
//    void sync() {
//        syncAgencies();
//        syncSubjects();
//        syncCollections();
//        syncTitles();
//    }
//
//    void syncAgencies() {
//        for (PandasAgency panAgency : pandasDAO.listAgencies()) {
//            Agency agency = agencies.getByLegacyIdOrNull(AGENCY.id(), panAgency.getId());
//            if (agency == null) {
//                agency = new Agency();
//            }
//
//            agency.setName(panAgency.getName());
//            agency.setLogo(panAgency.getLogo());
//            agency.setLegacyId(AGENCY.id(), panAgency.getId());
//            agency.setUrl(panAgency.getUrl());
//
//            agencies.save(agency);
//        }
//
//    }
//
//    /**
//     * For each subject:
//     * - Create a corresponding category with the same name
//     * - If the subject has a parent subject, link the new category to the corresponding parent category.
//     */
//    void syncSubjects() {
//        for (PandasSubject subject : pandasDAO.listSubjects()) {
//            Category category = categories.getByLegacyIdOrNull(SUBJECT.id(), subject.getId());
//            if (category == null) {
//                category = new Category();
//            }
//
//            category.setName(subject.getName());
//            category.setLegacyId(SUBJECT.id(), subject.getId());
//
//            Category parent = categories.getByLegacyId(SUBJECT.id(), subject.getParentId());
//            category.setParentId(parent == null ? null : parent.getId());
//
//            categories.save(category);
//        }
//    }
//
//    /**
//     * For each collection:
//     * - Create a corresponding category with the same name
//     * - TODO Add curating agencies to the category based on the owner of each title in the collection.
//     * - If the collection has a parent collection, link the new category to the corresponding parent category.
//     * - For each subject the collection is in:
//     *   + If there was a parent collection and it has this same subject, ignore this subject.
//     *   + If the new category has no parent yet, set its parent to this subject.
//     *   + Otherwise, since the category already has a parent, add a reference from the subject.
//     */
//    void syncCollections() {
//        for (PandasCollection collection : pandasDAO.listCollections()) {
//            Category category = categories.getByLegacyIdOrNull(COLLECTION.id(), collection.getId());
//            if (category == null) {
//                category = new Category();
//            }
//
//            category.setName(collection.getName());
//            category.setDescription(collection.getDisplayComment());
//            category.setLegacyId(COLLECTION.id(), collection.getId());
//
//            Category parent = categories.getByLegacyIdOrNull(COLLECTION.id(), collection.getParentId());
//            Set<Long> parentSubjectIds = Collections.emptySet();
//            if (parent != null) {
//                category.setParentId(parent.getId());
//                parentSubjectIds = pandasDAO.listSubjectsForCollectionId(collection.getId()).stream()
//                        .map(PandasSubject::getId).collect(toSet());
//            }
//
//            List<Long> categoriesToLinkFrom = new ArrayList<>();
//            for (PandasSubject subject : pandasDAO.listSubjectsForCollectionId(collection.getId())) {
//                if (!parentSubjectIds.contains(subject.getId())) {
//                    Category subjectCategory = categories.getByLegacyIdOrNull(SUBJECT.id(), subject.getId());
//                    if (category.getParentId() == null) {
//                        category.setParentId(subjectCategory.getId());
//                    } else {
//                        categoriesToLinkFrom.add(subjectCategory.getId());
//                    }
//                }
//            }
//
//            categories.save(category);
//
//            for (long categoryId : categoriesToLinkFrom) {
//                categories.addSymLinkIfNotExists(categoryId, category.getId());
//            }
//        }
//    }
//
// /**
//    For each title
//  - If the title has issues
//    + Create a category for the title
//    + Create a subcategory for each issue group
//    + Add each issue as an entry in the appropriate category
//    + Ignore the instances
//  - Else if the title has multiple instances with different domains
//    + Create a category for the title
//    + Create entries in the category for each variation
//      - Name these entries using the domain and year ranges: "Jason's blog (geocities.com) 1998-2002", "Jason's blog (jason.id.au) 2003-"
//            - Otherwise if the title has instances all sharing the same domain
//    + Create a directory entry for the title
//  - For each collection the title is in:
//            + If the title has no parent, set the parent to the be collection.
//            + Otherwise, since the title already has a parent add a reference from the collection.
//  - For each subject the title is in that is in the "subjects we want to treat as a collection" list:
//            + Apply the above collection rules.
// */
//    private void syncTitles() {
//        try (ResultIterator<PandasTitle> it = pandasDAO.iterateTitles()) {
//            while (it.hasNext()) {
//                PandasTitle title = it.next();
//
//
//            }
//        }
//    }
//

}
