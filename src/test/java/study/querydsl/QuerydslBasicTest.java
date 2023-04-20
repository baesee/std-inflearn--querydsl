package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;


@SpringBootTest
@Transactional
public class QuerydslBasicTest {
    @Autowired
    EntityManager em;
    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);
        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJQPL(){
        // member1를 찾아라.
        String qlString = "select m from Member m where m.username = :username";
        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void startQuerydsl(){
        //QMember m = new QMember("m");
        //QMember m = QMember.member;

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))  // 파라미터 바인딩 처리
                .fetchOne();

        assertEquals(findMember.getUsername(), "member1");

    }

    @Test
    public void search(){
        Member findMember = queryFactory.selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10,30)))
                .fetchOne();

        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void searchAndParam(){
        Member findMember = queryFactory.selectFrom(member)
                .where(
                        member.username.eq("member1")
                        ,member.age.eq(10) // ',' 의 경우 'and'
                )
                .fetchOne();

        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void resultFetch(){
        List<Member> fetch = queryFactory.selectFrom(member)
                .fetch();

        Member member1 = queryFactory.selectFrom(member).fetchOne();

        List<Member> fetch1 = queryFactory.selectFrom(member).fetch();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 desc
     * 2. 회원 이름 올림차순 asc
     * 단, 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    public void sort(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

    }

    @Test
    public void paging1(){
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        int size = result.size();
        System.out.println("size = " + size);
    }

    @Test
    public void paging2(){
        QueryResults<Member> queryRestuls = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        long total = queryRestuls.getTotal();
        System.out.println("total = " + total);
        long limit = queryRestuls.getLimit();
        System.out.println("limit = " + limit);
        long offset = queryRestuls.getOffset();
        System.out.println("offset = " + offset);
        int size = queryRestuls.getResults().size();
        System.out.println("size = " + size);

    }

    @Test
    public void aggregation() throws Exception {
        List<Tuple> result = queryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();
        Tuple tuple = result.get(0);
        assertEquals(tuple.get(member.count()),4);
        assertEquals(tuple.get(member.age.sum()), 100);
        assertEquals(tuple.get(member.age.avg()), 25);
        assertEquals(tuple.get(member.age.max()),40);
        assertEquals(tuple.get(member.age.min()), 10);

    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구하라
     * @throws Exception
     */
    @Test
    public void groupBy() throws Exception{
        // given
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                //.having()
                .fetch();
        // when
        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        // then
        assertEquals(teamA.get(team.name), "teamA");
        assertEquals(teamA.get(member.age.avg()), 15);

        assertEquals(teamB.get(team.name), "teamB");
        assertEquals(teamB.get(member.age.avg()), 35);
    }

    /**
     * 팀 A에 소속된 모든 회원
     * @throws Exception
     */
    @DisplayName("JoinTest")
    @Test
    public void join() throws Exception{
        // given
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();
        // when

        // then
    }

    /**
     * 쎄타(theta_join)조인  : 연관관계가 없는 테이블끼리 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * @throws Exception
     */
    @DisplayName("쎼타조인(theta_join)")
    @Test
    public void theta_join() throws Exception{
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // when
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        // then

    }

    /**
     * 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: SELECT m, t FROM Member m LEFT JOIN m.team t on t.name = 'teamA'
     * SQL: SELECT m.*, t.* FROM Member m LEFT JOIN Team t ON m.TEAM_ID=t.id and
     * @throws Exception
     */
    @DisplayName("조인 ON 필터")
    @Test
    public void join_on_filtering() throws Exception{
        // given
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();
        // when

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
        // then
    }

    /**
     * 연관관계가 없는 엔티티 외부조인
     * 회원의 이름이 팀 이름과 같은 대상 외부조인
     * @throws Exception
     */
    @DisplayName("연관관계가 없는 테이블끼리 조인")
    @Test
    public void join_on_no_relation() throws Exception{
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        // when
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        // then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @DisplayName("fetch join no")
    @Test
    public void fetchJoinNO() throws Exception{
        // given
        em.flush();
        em.clear();
        Member findMember = queryFactory.selectFrom(member).where(member.username.eq("member1")).fetchOne();
        System.out.println("findMember = " + findMember);
        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertFalse(loaded);

        // then
    }

    @DisplayName("fetch join use")
    @Test
    public void fetchJoinUSE() throws Exception{
        // given
        em.flush();
        em.clear();
        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();
        System.out.println("findMember = " + findMember);
        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertTrue(loaded);

        // then
    }

    /**
     * 나이가 가장 많은 회원 조회
     * @throws Exception
     */
    @DisplayName("서브쿼리")
    @Test
    public void subQuery() throws Exception{
        // given

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
        // when
        // then
        assertEquals(result.get(0).getAge(), 40);
    }

    /**
     * 나이가 평균 이상인 회원
     * @throws Exception
     */
    @DisplayName("서브쿼리 >=")
    @Test
    public void subQueryGoe() throws Exception{
        // given

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        // when
        // then
        //assertEquals(result.get(0).getAge(), 40);
    }

    /**
     * 나이가 평균 이상인 회원
     * @throws Exception
     */
    @DisplayName("서브쿼리 IN")
    @Test
    public void subQueryIN() throws Exception{
        // given

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();
        // when
        // then
        //assertEquals(result.get(0).getAge(), 40);
    }

    /**
     * 나이가 평균 이상인 회원
     * @throws Exception
     */
    @DisplayName("SELECT 절에서 서브쿼리")
    @Test
    public void selectSubQuery() throws Exception{
        // given

        QMember memberSub = new QMember("memberSub");
        List<Tuple> result = queryFactory.select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                ).from(member)
                .fetch();
        // when
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);

        }

        // then
        //assertEquals(result.get(0).getAge(), 40);
    }

    /**
     *
     */
    @DisplayName("SIMPLE Case When then")
    @Test
    public void basicCase(){
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("나이 = " + s);
        }
    }

    @DisplayName("COMPLEX Case When then")
    @Test
    public void complexCase(){
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21,30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

        /*
            예를 들어서 다음과 같은 임의의 순서로 회원을 출력하고 싶다면?
            1. 0 ~ 30살이 아닌 회원을 가장 먼저 출력
            2. 0 ~ 20살 회원 출력
            3. 21 ~ 30살 회원 출력
         */
        NumberExpression<Integer> rankPath = new CaseBuilder()
                .when(member.age.between(0, 20)).then(2)
                .when(member.age.between(21, 30)).then(1)
                .otherwise(3);
        List<Tuple> result2 = queryFactory
                .select(member.username, member.age, rankPath)
                .from(member)
                .orderBy(rankPath.desc())
                .fetch();
        for (Tuple tuple : result2) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            Integer rank = tuple.get(rankPath);
            System.out.println("username = " + username + " age = " + age + " rank = "
                    + rank); }

    }
    
    @DisplayName("상수 처리(Expressions.constant)")
    @Test
    public void constant() throws Exception{
        // given
        List<Tuple> a = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : a) {
            System.out.println("tuple = " + tuple);
        }

        // when
    
        // then
    }

    @DisplayName("문자열 더하기")
    @Test
    public void concat() throws Exception{
        // given
        // 예상 : username_age
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
        // when

        // then
    }

    @Test
    public void simpleProjection(){
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void tupleProjection(){
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            System.out.println("username = " + username);
            Integer age = tuple.get(member.age);
            System.out.println("age = " + age);
        }
    }


    //===============================================================================
    // > > > > > > > > > > > > > > > > > > DTO로 조회 하기 (dto Projection) START
    //===============================================================================

    /**
     * 일단 먼저 JPQL로 해보자
     */
    @Test
    public void findDtoByJPQL(){
        List<MemberDto> resultList = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : resultList) {
            System.out.println("memberDto = " + memberDto);
        }
    }


    //=============================================
    // QeuryDSL을 이용하여 Setter방식으로 DTO로 조회 하기
    // ex) select(Projections.bean(MemberDto.class,
    //        member.username,
    //        member.age))
    // 이렇게 사용한다.
    // ## [ 특의사항 ] ##
    // @Getter, @Setter 가 필수로 필요 하며 기본생성자(빈생성자) 필수이다
    //=============================================
    @DisplayName("QeuryDSL > Setter방식으로 DTO 조회")
    @Test
    public void findDtoBySetter(){
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

    }


    //=============================================
    // QeuryDSL을 이용하여 Feild방식으로 DTO로 조회 하기
    // ex) select(Projections.fields(MemberDto.class,
    //        member.username,
    //        member.age))
    // 이렇게 사용한다.
    // ## [ 특의사항 ] ##
    // .bean 형태와는 다르게 getter, setter, 기본 생성자 필요없이 그냥 바로 필드에 대입되어 값이 세팅된다
    //=============================================
    @DisplayName("QeuryDSL > Field 방식으로  DTO 조회")
    @Test
    public void findDtoByField(){
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //=============================================
    // QeuryDSL을 이용하여 생성자(Constructor) DTO로 조회 하기
    // ex) select(Projections.((MemberDto.class,
    //        member.username,
    //        member.age))
    // 이렇게 사용한다.
    // ## [ 특의사항 ] ##
    // .bean 형태와는 다르게 getter, setter, 기본 생성자 필요없이 그냥 바로 필드에 대입되어 값이 세팅된다
    //=============================================
    @DisplayName("QeuryDSL > 생성자(Constructor)방식으로 DTO 조회")
    @Test
    public void findDtoByConstructor(){
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //=============================================
    // QeuryDSL을 이용하여 Feild방식으로 별칭이다른 DTO로 조회 하기
    // ## [ 특의사항 ] ##
    // 필드와 필드의 별칭이 다를 경우
    // 실제 db 컬럼 username, Dto 필드 name
    //  -> member.username.as("name"),
    // 위와 같이 별칭을 주면 된다.
    // 서브쿼리 별칭, subquery alias , as
    //=============================================
    @DisplayName("QeuryDSL > 별칭이 다른 경우 DTO 조회")
    @Test
    public void findUserDto(){

        QMember memberSub = new QMember(("memberSub"));

        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")
                        //member.age
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    //=============================================
    // !! Best !!
    // Dto생성자에 @QueryProjection 어노테이션 추가 후 진행
    //=============================================
    @DisplayName("QeuryDSL > @QueryProjection를 이용한 DTO 조회")
    @Test
    public void findDtoByQueryProjection(){

        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //===============================================================================
    // DTO로 조회 하기 (dto Projection) END < < < < < < < < < < < < < < < < < < <
    //===============================================================================

    //===============================================================================
    // > > > > > > > > > > > > > > > > > > > > > 동적쿼리 START
    //===============================================================================

    @DisplayName("동적쿼리 불리언 빌더 방식 (BooleanBuilder)")
    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception{
        // given
        String usernameParam = "member1";
        Integer ageParam = null;
        //Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        // when
    
        // then
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();
        if(usernameCond != null){
            builder.and(member.username.eq(usernameCond));
        }
        if(ageCond != null){
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @DisplayName("동적쿼리 다중 파라미터 방식(Best!)")
    @Test
    public void dynamicQuery_WhereParam() throws Exception{
        // given
        String usernameParam = "member1";
        Integer ageParam = 10;
        //Integer ageParam = null;

        List<Member> result = searchMember2(usernameParam, ageParam);
        // when

        // then
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(member)
                //.where(usernameEq(usernameCond), ageEq(ageCond))
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond): null ;
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond): null ;
    }

    private BooleanExpression allEq(String usernaeCond, Integer ageCond) {
        return usernameEq(usernaeCond).and(ageEq(ageCond));
    }

    //===============================================================================
    // 동적쿼리 END < < < < < < < < < < < < < < < < < < < < < < < <
    //===============================================================================

    @Test
    public void bulkUpdate(){
        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        em.flush();
        em.clear();

        List<Member> list = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : list) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    public void bulkAdd(){
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();
    }

    @Test
    public void bulkDelete(){
        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    public void sqlFunction(){
        List<String> result = queryFactory
                .select(Expressions.stringTemplate("function('replace', {0}, {1}, {2})", member.username, "member", "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void sqlFunction2(){
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                //.where(member.username.eq(Expressions.stringTemplate("function('lower', {0})", member.username)))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

}